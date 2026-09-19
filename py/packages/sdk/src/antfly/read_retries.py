"""Opt-in retries for query requests rejected explicitly before execution."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import httpx

from .admission import AdmissionAsyncHTTPClient, AdmissionHTTPClient, AdmissionPool

_QUERY = re.compile(r"/db/v1/(query|tables/[^/]+/query|databases/[^/]+/namespaces/[^/]+/tables/[^/]+/query)")


@dataclass(frozen=True)
class ReadRetryPolicy:
    """Seconds; max_attempts includes the original. Queries can see newer data.

    The original caller deadline (antfly_deadline request extension, monotonic
    seconds) wins over max_elapsed. Async task cancellation interrupts backoff.
    Async streamed response reads retain that absolute deadline after headers.
    Query timeout_ms also bounds the original operation; subsequent dispatches
    forward only the remaining body budget without reencoding other fields.
    Synchronous httpx I/O timeouts are capped by the remaining budget; as with
    httpx itself they bound individual I/O waits, not total stream consumption.
    """

    max_attempts: int
    max_elapsed: float
    initial_backoff: float
    max_backoff: float

    def __post_init__(self) -> None:
        if (
            type(self.max_attempts) is not int
            or not 2 <= self.max_attempts <= 5
            or not all(math.isfinite(value) for value in (self.max_elapsed, self.initial_backoff, self.max_backoff))
            or not 0 < self.initial_backoff <= self.max_backoff <= self.max_elapsed <= 60
        ):
            raise ValueError("invalid Antfly read retry policy")


def _eligible(request: httpx.Request) -> bool:
    if request.method != "POST" or not _QUERY.fullmatch(request.url.raw_path.decode().split("?", 1)[0]):
        return False
    try:
        request.content  # Streaming/non-replayable requests bypass retries.
    except httpx.RequestNotRead:
        return False
    return True


@dataclass
class _QueryBody:
    budget: float
    text: str
    timeout_spans: list[tuple[int, int]]


def _query_body(request: httpx.Request, maximum: float) -> _QueryBody | None:
    if len(request.content) > 1 << 20:
        return None
    try:
        text = request.content.decode("utf-8")
        ndjson = request.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/x-ndjson"
        lines = text.splitlines(keepends=True) if ndjson else [text]

        def invalid_constant(value: str) -> None:
            raise ValueError(f"non-JSON constant: {value}")

        decoder = json.JSONDecoder(parse_constant=invalid_constant)
        spans = []
        offset = 0
        seen = False
        for line in lines:
            if not line.strip():
                offset += len(line)
                continue
            value = decoder.decode(line)
            if not isinstance(value, dict):
                return None
            seen = True
            timeout = value.get("timeout_ms")
            if timeout is not None:
                if type(timeout) is not int or not 0 <= timeout <= (1 << 64) - 1:
                    return None
                maximum = min(maximum, timeout / 1000)
            # Replace only top-level timeout tokens. Every other byte, including
            # large integers, decimal spelling and unknown fields, is preserved.
            position = len(line) - len(line.lstrip()) + 1
            keys = set()
            while True:
                position += len(line[position:]) - len(line[position:].lstrip())
                if line[position] == "}":
                    break
                key, position = decoder.raw_decode(line, position)
                if key in keys:
                    return None
                keys.add(key)
                position += len(line[position:]) - len(line[position:].lstrip())
                position += 1
                position += len(line[position:]) - len(line[position:].lstrip())
                start = position
                _, position = decoder.raw_decode(line, position)
                if key == "timeout_ms" and timeout is not None:
                    spans.append((offset + start, offset + position))
                position += len(line[position:]) - len(line[position:].lstrip())
                if line[position] == "}":
                    break
                position += 1
            offset += len(line)
        return _QueryBody(maximum, text, spans) if seen else None
    except (ValueError, UnicodeError, IndexError, TypeError):
        return None


def _bounded(response: httpx.Response) -> bool:
    length = response.headers.get("Content-Length", "")
    return (
        response.status_code == 429
        and len(length) <= 5
        and length.isascii()
        and length.isdecimal()
        and int(length) <= 16384
    )


def _delay(
    policy: ReadRetryPolicy, response: httpx.Response, body: bytes, attempt: int, deadline: float
) -> float | None:
    if len(body) > 16384:
        return None
    try:
        detail = json.loads(body)
    except (ValueError, UnicodeError):
        return None
    if (
        not isinstance(detail, dict)
        or detail.get("reason") != "instance_busy"
        or detail.get("stage") != "admission"
        or detail.get("execution_started") is not False
    ):
        return None
    delay = min(policy.initial_backoff * 2**attempt, policy.max_backoff)
    after = response.headers.get("Retry-After")
    if after is not None:
        if len(after) > 2 or not after.isascii() or not after.isdecimal() or int(after) > policy.max_backoff:
            return None
        delay = max(delay, int(after))
    return delay if time.monotonic() + delay < deadline else None


def _request(request: httpx.Request, deadline: float, body: _QueryBody) -> httpx.Request:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise httpx.TimeoutException("Antfly read retry deadline expired", request=request)
    extensions = dict(request.extensions)
    extensions["antfly_deadline"] = deadline
    extensions["timeout"] = {
        key: min(value, remaining) if value is not None else remaining
        for key, value in extensions.get(
            "timeout", {"connect": None, "read": None, "write": None, "pool": None}
        ).items()
    }
    text = body.text
    for start, end in reversed(body.timeout_spans):
        text = text[:start] + str(max(0, math.floor(remaining * 1000))) + text[end:]
    headers = httpx.Headers(request.headers)
    headers.pop("Content-Length", None)
    return httpx.Request(
        request.method, request.url, headers=headers, content=text.encode("utf-8"), extensions=extensions
    )


class _ReplaySync(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], tail: Iterator[bytes], original: httpx.SyncByteStream):
        self.chunks, self.tail, self.original = chunks, tail, original

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks
        yield from self.tail

    def close(self) -> None:
        self.original.close()


class _ReplayAsync(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], tail: AsyncIterator[bytes], original: httpx.AsyncByteStream):
        self.chunks, self.tail, self.original = chunks, tail, original

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        async for chunk in self.tail:
            yield chunk

    async def aclose(self) -> None:
        await self.original.aclose()


class _DeadlineAsync(httpx.AsyncByteStream):
    """Keep the original operation budget after send returns response headers."""

    _cleanup_tasks: set[asyncio.Task[None]] = set()

    def __init__(self, original: httpx.AsyncByteStream, deadline: float):
        self.original, self.deadline = original, deadline
        self.closing: asyncio.Task[None] | None = None

    async def __aiter__(self) -> AsyncIterator[bytes]:
        iterator = self.original.__aiter__()
        try:
            while True:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Antfly query response deadline expired")
                try:
                    # Scope cancellation to the transport read, never across a
                    # yield into unrelated consumer work or a different task.
                    async with asyncio.timeout(remaining):
                        chunk = await anext(iterator)
                except StopAsyncIteration:
                    return
                if time.monotonic() >= self.deadline:
                    raise TimeoutError("Antfly query response deadline expired")
                yield chunk
        finally:
            self._start_close()
            task = asyncio.current_task()
            if task is None or not task.cancelling():
                await self.aclose()

    def _start_close(self) -> None:
        if self.closing is None:
            self.closing = asyncio.create_task(self.original.aclose())
            self._cleanup_tasks.add(self.closing)

            def done(task: asyncio.Task[None]) -> None:
                self._cleanup_tasks.discard(task)
                if not task.cancelled():
                    task.exception()

            self.closing.add_done_callback(done)

    async def aclose(self) -> None:
        self._start_close()
        assert self.closing is not None
        # Repeated caller cancellation must not abandon transport cleanup or
        # release the inner admission permit before that cleanup finishes.
        await asyncio.shield(self.closing)


class ReadRetryHTTPClient(AdmissionHTTPClient):
    def __init__(self, policy: ReadRetryPolicy, pool: AdmissionPool | None = None, **kwargs: Any):
        super().__init__(pool, **kwargs)
        self.read_retry_policy = policy

    def send(self, request: httpx.Request, *, stream: bool = False, **kwargs: Any) -> httpx.Response:
        if not _eligible(request):
            return super().send(request, stream=stream, **kwargs)
        policy = self.read_retry_policy
        started = time.monotonic()
        query_body = _query_body(request, policy.max_elapsed)
        if query_body is None:
            return super().send(request, stream=stream, **kwargs)
        deadline = min(started + query_body.budget, request.extensions.get("antfly_deadline", math.inf))
        for attempt in range(policy.max_attempts):
            response = super().send(_request(request, deadline, query_body), stream=True, **kwargs)
            try:
                delay = None
                if attempt + 1 < policy.max_attempts and _bounded(response):
                    if response.is_closed:
                        body = response.content
                    else:
                        assert isinstance(response.stream, httpx.SyncByteStream)
                        original = response.stream
                        tail = iter(original)
                        chunks: list[bytes] = []
                        size = 0
                        for chunk in tail:
                            chunks.append(chunk)
                            size += len(chunk)
                            if size > 16384:
                                break
                        response.stream = _ReplaySync(chunks, tail, original)
                        body = b"".join(chunks) if size <= 16384 else b"x" * 16385
                    delay = _delay(policy, response, body, attempt, deadline)
                if delay is None:
                    if not stream:
                        response.read()
                        response.close()
                    return response
                response.close()
                time.sleep(delay)
            except BaseException:
                response.close()
                raise
        raise AssertionError("unreachable")


class ReadRetryAsyncHTTPClient(AdmissionAsyncHTTPClient):
    def __init__(self, policy: ReadRetryPolicy, pool: AdmissionPool | None = None, **kwargs: Any):
        super().__init__(pool, **kwargs)
        self.read_retry_policy = policy

    async def send(self, request: httpx.Request, *, stream: bool = False, **kwargs: Any) -> httpx.Response:
        if not _eligible(request):
            return await super().send(request, stream=stream, **kwargs)
        policy = self.read_retry_policy
        started = time.monotonic()
        query_body = _query_body(request, policy.max_elapsed)
        if query_body is None:
            return await super().send(request, stream=stream, **kwargs)
        deadline = min(started + query_body.budget, request.extensions.get("antfly_deadline", math.inf))
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            for attempt in range(policy.max_attempts):
                response = await super().send(_request(request, deadline, query_body), stream=True, **kwargs)
                try:
                    delay = None
                    if attempt + 1 < policy.max_attempts and _bounded(response):
                        if response.is_closed:
                            body = response.content
                        else:
                            assert isinstance(response.stream, httpx.AsyncByteStream)
                            original = response.stream
                            tail = original.__aiter__()
                            chunks: list[bytes] = []
                            size = 0
                            async for chunk in tail:
                                chunks.append(chunk)
                                size += len(chunk)
                                if size > 16384:
                                    break
                            response.stream = _ReplayAsync(chunks, tail, original)
                            body = b"".join(chunks) if size <= 16384 else b"x" * 16385
                        delay = _delay(policy, response, body, attempt, deadline)
                    if delay is None:
                        if stream and not response.is_closed:
                            assert isinstance(response.stream, httpx.AsyncByteStream)
                            response.stream = _DeadlineAsync(response.stream, deadline)
                        if not stream:
                            await response.aread()
                            await response.aclose()
                        return response
                    await response.aclose()
                    await asyncio.sleep(delay)
                except BaseException:
                    await response.aclose()
                    raise
        raise AssertionError("unreachable")
