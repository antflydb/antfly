"""Bounded FIFO admission for one synchronous SDK client."""

import math
import threading
import time
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager

from .exceptions import ClientCapacityError


class Admission:
    def __init__(self, active: int, queued: int, max_wait: float) -> None:
        if type(active) is not int or active < 1:
            raise ValueError("max_concurrent_requests must be a positive integer")
        if type(queued) is not int or queued < 0:
            raise ValueError("max_queued_requests must be a non-negative integer")
        if isinstance(max_wait, bool) or not math.isfinite(max_wait) or max_wait < 0:
            raise ValueError("max_queue_wait must be finite and non-negative")
        self._capacity = active
        self._queue_capacity = queued
        self._max_wait = max_wait
        self._condition = threading.Condition()
        self._waiters: deque[object] = deque()
        self._active = 0
        self._closed = False

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    @contextmanager
    def acquire(self, deadline: float) -> Iterator[None]:
        with self._condition:
            if self._closed:
                raise RuntimeError("Cannot send a request, as the client has been closed.")
            if time.monotonic() >= deadline:
                raise ClientCapacityError("deadline_exceeded")
            if not self._waiters and self._active < self._capacity:
                self._active += 1
            else:
                if len(self._waiters) >= self._queue_capacity:
                    raise ClientCapacityError("queue_full")
                if self._max_wait == 0:
                    raise ClientCapacityError("wait_timeout")
                waiter = object()
                self._waiters.append(waiter)
                until = min(deadline, time.monotonic() + self._max_wait)
                try:
                    while True:
                        if self._closed:
                            raise RuntimeError("Cannot send a request, as the client has been closed.")
                        remaining = until - time.monotonic()
                        if remaining <= 0:
                            reason = "deadline_exceeded" if until == deadline else "wait_timeout"
                            raise ClientCapacityError(reason)
                        if self._waiters[0] is waiter and self._active < self._capacity:
                            self._active += 1
                            break
                        self._condition.wait(remaining)
                finally:
                    # Cancellation/interrupt, expiry and close retire their own
                    # entry. Waking everyone also lets a newly eligible head run.
                    self._waiters.remove(waiter)
                    self._condition.notify_all()
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()
