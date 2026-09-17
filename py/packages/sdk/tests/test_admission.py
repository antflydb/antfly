import asyncio
import threading
import time

import httpx
import pytest

from antfly import (
    AdmissionAsyncHTTPClient,
    AdmissionHTTPClient,
    AdmissionPool,
    AntflyClient,
    ClientAdmission,
    ClientBusyError,
)


def test_sync_stream_holds_slot_and_waiting_expires_without_dispatch() -> None:
    pool = AdmissionPool(ClientAdmission(1, 1, 0.02))
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, stream=httpx.ByteStream(b"payload"))

    with AdmissionHTTPClient(pool, transport=httpx.MockTransport(handle), base_url="http://localhost") as client:
        with client.stream("GET", "/active") as response:
            with pytest.raises(ClientBusyError):
                client.get("/expired")
            assert pool.stats == {"active": 1, "queued": 0}
            assert response.read() == b"payload"
            assert pool.stats == {"active": 0, "queued": 0}
        assert client.get("/next").content == b"payload"
    assert calls == ["/active", "/next"]


def test_sync_waiter_wakes_and_preserves_thread_safety() -> None:
    pool = AdmissionPool(ClientAdmission(1, 1, 2))
    active = pool.acquire()
    started = threading.Event()
    errors: list[BaseException] = []

    def waiter() -> None:
        try:
            lease = pool.acquire()
            started.set()
            lease.release()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    deadline = time.monotonic() + 1
    while pool.stats["queued"] == 0 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert pool.stats == {"active": 1, "queued": 1}
    with pytest.raises(ClientBusyError):
        pool.acquire()
    active.release()
    thread.join(2)
    assert not thread.is_alive()
    assert started.is_set() and not errors
    assert pool.stats == {"active": 0, "queued": 0}


@pytest.mark.asyncio
async def test_async_cancel_racing_grant_releases_once() -> None:
    pool = AdmissionPool(ClientAdmission(1, 1, 1))
    for _ in range(50):
        active = pool.acquire()
        queued = asyncio.create_task(pool.acquire_async())
        await asyncio.sleep(0)
        assert pool.stats["queued"] == 1
        active.release()
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
        assert pool.stats == {"active": 0, "queued": 0}


@pytest.mark.asyncio
async def test_async_stream_cleanup_remains_charged_after_caller_cancellation() -> None:
    pool = AdmissionPool(ClientAdmission(1, 1, 1))
    closing = asyncio.Event()
    finish = asyncio.Event()

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"payload"

        async def aclose(self):
            closing.set()
            await finish.wait()

    async def handle(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Stream())

    async with AdmissionAsyncHTTPClient(pool, transport=httpx.MockTransport(handle)) as client:
        response = await client.send(httpx.Request("GET", "http://localhost"), stream=True)
        close = asyncio.create_task(response.aclose())
        await closing.wait()
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        assert pool.stats["active"] == 1
        finish.set()
        await asyncio.gather(*pool._cleanup_tasks)
        assert pool.stats == {"active": 0, "queued": 0}


def test_unknown_write_failure_is_not_retried() -> None:
    calls = 0
    pool = AdmissionPool(ClientAdmission(1))

    def handle(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadError("response lost")

    with AdmissionHTTPClient(pool, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(httpx.ReadError):
            client.post("http://localhost", json={})
    assert calls == 1
    assert pool.stats == {"active": 0, "queued": 0}


@pytest.mark.asyncio
async def test_high_level_client_keeps_auth_and_shares_sync_async_pool() -> None:
    pool = AdmissionPool(ClientAdmission(1))
    client = AntflyClient("http://localhost", token="test-token", admission=pool)
    sync = client._client.get_httpx_client()
    asynchronous = client._client.get_async_httpx_client()
    try:
        assert isinstance(sync, AdmissionHTTPClient)
        assert isinstance(asynchronous, AdmissionAsyncHTTPClient)
        assert sync.headers["Authorization"] == "Bearer test-token"
        assert asynchronous.headers["Authorization"] == "Bearer test-token"
        assert sync.admission_pool is asynchronous.admission_pool is pool
        with pytest.raises(ClientBusyError):
            await asynchronous.get("http://localhost", extensions={"antfly_deadline": 0})
    finally:
        sync.close()
        await asynchronous.aclose()


def test_configuration_and_expired_deadlines() -> None:
    for value in (0, -1, 1.5, True):
        with pytest.raises(ValueError):
            ClientAdmission(value)
    with pytest.raises(ValueError):
        ClientAdmission(1, 1, 0)
    with pytest.raises(ValueError):
        ClientAdmission(1, 1, float("nan"))
    pool = AdmissionPool(ClientAdmission(1))
    with pytest.raises(ClientBusyError):
        pool.acquire(time.monotonic() - 1)
    assert pool.stats == {"active": 0, "queued": 0}
