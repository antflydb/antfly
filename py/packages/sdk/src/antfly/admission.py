"""Bounded reusable client admission; response streams retain their reservations."""

import asyncio
import math
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx

from .exceptions import AntflyException


class ClientBusyError(AntflyException):
    """This attempt was not dispatched. Earlier attempts may have executed."""

    execution_started = False


@dataclass(frozen=True)
class ClientAdmission:
    max_in_flight: int
    max_queued: int = 0
    max_wait: float = 0.0

    def __post_init__(self) -> None:
        if (
            type(self.max_in_flight) is not int
            or self.max_in_flight <= 0
            or type(self.max_queued) is not int
            or self.max_queued < 0
            or not math.isfinite(self.max_wait)
            or self.max_wait < 0
            or (self.max_queued > 0 and self.max_wait == 0)
        ):
            raise ValueError("invalid Antfly client admission limits")


@dataclass(eq=False)
class _Waiter:
    deadline: float
    wake: Callable[[], None]
    granted: bool = False
    retired: bool = False


class _Lease:
    def __init__(self, pool: "AdmissionPool", waiter: _Waiter) -> None:
        self.pool = pool
        self.waiter = waiter

    def release(self) -> None:
        self.pool._retire(self.waiter)


class AdmissionPool:
    """One count bound shared by synchronous threads and asyncio clients.

    Waiting is FIFO, subject to cancellation/expiry. No automatic retries are
    added. Reuse a pool across database/inference clients when they share a
    resource envelope; each server still enforces its own limits.
    """

    def __init__(self, config: ClientAdmission) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._active = 0
        self._queue: deque[_Waiter] = deque()
        # Cancellation may return before local stream cleanup; its slot remains
        # charged until this bounded, strongly owned cleanup task completes.
        self._cleanup_tasks: set[asyncio.Task[None]] = set()

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"active": self._active, "queued": len(self._queue)}

    def _enqueue(self, wake: Callable[[], None], deadline: float | None) -> _Waiter:
        now = time.monotonic()
        if deadline is not None and deadline <= now:
            raise ClientBusyError("Antfly client request deadline expired before dispatch")
        waiter = _Waiter(min(deadline or math.inf, now + self.config.max_wait), wake)
        with self._lock:
            if not self._queue and self._active < self.config.max_in_flight:
                self._active += 1
                waiter.granted = True
                return waiter
            if len(self._queue) >= self.config.max_queued or self.config.max_wait == 0:
                raise ClientBusyError("Antfly client admission queue full")
            self._queue.append(waiter)
        return waiter

    def _retire(self, waiter: _Waiter) -> None:
        with self._lock:
            if waiter.retired:
                return
            waiter.retired = True
            if waiter.granted:
                self._active -= 1
            else:
                # Expired entries may already have been removed by the pump.
                try:
                    self._queue.remove(waiter)
                except ValueError:
                    pass
            while self._queue and self._active < self.config.max_in_flight:
                candidate = self._queue.popleft()
                if time.monotonic() < candidate.deadline:
                    candidate.granted = True
                    self._active += 1
                # These are internal Event/Future notifications only. A waiter
                # reacquires the lock before returning or retiring its charge.
                try:
                    candidate.wake()
                except RuntimeError:
                    # A stopped asyncio loop cannot consume a grant. Retire it
                    # here instead of leaking capacity into a dead destination.
                    if candidate.granted:
                        self._active -= 1
                    candidate.retired = True

    def _accepted(self, waiter: _Waiter, *, waited: bool) -> _Lease:
        with self._lock:
            granted = waiter.granted and (not waited or time.monotonic() < waiter.deadline)
        if not granted:
            raise ClientBusyError("Antfly client admission wait expired")
        return _Lease(self, waiter)

    def acquire(self, deadline: float | None = None) -> _Lease:
        event = threading.Event()
        waiter = self._enqueue(event.set, deadline)
        waited = not waiter.granted
        try:
            if waited:
                event.wait(max(0, waiter.deadline - time.monotonic()))
            return self._accepted(waiter, waited=waited)
        except BaseException:
            self._retire(waiter)
            raise

    async def acquire_async(self, deadline: float | None = None) -> _Lease:
        loop = asyncio.get_running_loop()
        ready: asyncio.Future[None] = loop.create_future()

        def deliver() -> None:
            if not ready.done():
                ready.set_result(None)

        def wake() -> None:
            loop.call_soon_threadsafe(deliver)

        waiter = self._enqueue(wake, deadline)
        waited = not waiter.granted
        try:
            if waited:
                try:
                    await asyncio.wait_for(ready, max(0, waiter.deadline - time.monotonic()))
                except TimeoutError as error:
                    raise ClientBusyError("Antfly client admission wait expired") from error
            return self._accepted(waiter, waited=waited)
        except BaseException:
            self._retire(waiter)
            raise


class _SyncStream(httpx.SyncByteStream):
    def __init__(self, stream: httpx.SyncByteStream, lease: _Lease) -> None:
        self.stream, self.lease = stream, lease
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        try:
            yield from self.stream
        finally:
            self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.stream.close()
        finally:
            self.lease.release()


class _AsyncStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream, lease: _Lease) -> None:
        self.stream, self.lease = stream, lease
        self.closing: asyncio.Task[None] | None = None

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self.stream:
                yield chunk
        finally:
            await self.aclose()

    async def _close(self) -> None:
        try:
            await self.stream.aclose()
        finally:
            self.lease.release()

    async def aclose(self) -> None:
        if self.closing is None:
            task = asyncio.create_task(self._close())
            self.closing = task
            self.lease.pool._cleanup_tasks.add(task)

            def done(completed: asyncio.Task[None]) -> None:
                self.lease.pool._cleanup_tasks.discard(completed)
                if not completed.cancelled():
                    completed.exception()  # observe cleanup errors if caller canceled

            task.add_done_callback(done)
        await asyncio.shield(self.closing)


class AdmissionHTTPClient(httpx.Client):
    """httpx client preserving its connection/proxy pool and bounding operations."""

    def __init__(self, pool: AdmissionPool | None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.admission_pool = pool

    def send(self, request: httpx.Request, *, stream: bool = False, **kwargs: Any) -> httpx.Response:
        if self.admission_pool is None:
            return super().send(request, stream=stream, **kwargs)
        lease = self.admission_pool.acquire(request.extensions.get("antfly_deadline"))
        try:
            response = super().send(request, stream=True, **kwargs)
            assert isinstance(response.stream, httpx.SyncByteStream)
            if response.is_closed:
                lease.release()
            else:
                response.stream = _SyncStream(response.stream, lease)
        except BaseException:
            lease.release()
            raise
        if not stream:
            try:
                response.read()
            finally:
                response.close()
        return response


class AdmissionAsyncHTTPClient(httpx.AsyncClient):
    """asyncio httpx client; task cancellation retires queued work before dispatch."""

    def __init__(self, pool: AdmissionPool | None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.admission_pool = pool

    async def send(self, request: httpx.Request, *, stream: bool = False, **kwargs: Any) -> httpx.Response:
        if self.admission_pool is None:
            return await super().send(request, stream=stream, **kwargs)
        lease = await self.admission_pool.acquire_async(request.extensions.get("antfly_deadline"))
        try:
            response = await super().send(request, stream=True, **kwargs)
            assert isinstance(response.stream, httpx.AsyncByteStream)
            if response.is_closed:
                lease.release()
            else:
                response.stream = _AsyncStream(response.stream, lease)
        except BaseException:
            lease.release()
            raise
        if not stream:
            try:
                await response.aread()
            finally:
                await response.aclose()
        return response
