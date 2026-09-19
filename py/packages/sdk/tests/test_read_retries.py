import asyncio
import json
import time

import httpx
import pytest

from antfly import AdmissionPool, ClientAdmission, ReadRetryAsyncHTTPClient, ReadRetryHTTPClient, ReadRetryPolicy

REJECTION = {"reason": "instance_busy", "stage": "admission", "execution_started": False}
POLICY = ReadRetryPolicy(3, 1, 0.001, 0.01)
URL = "http://test/db/v1/tables/docs/query"


def test_preserves_oversized_or_chunked_error_body():
    for length in (None, "5"):
        calls = []
        body = b"x" * 17000

        def handle(request):
            calls.append(request)
            return httpx.Response(
                429, stream=httpx.ByteStream(body), headers={"Content-Length": length} if length else {}
            )

        with ReadRetryHTTPClient(POLICY, transport=httpx.MockTransport(handle)) as client:
            response = client.post(URL, content=b"{}")
        assert response.content == body
        assert len(calls) == 1


def test_retries_replay_and_release_admission():
    calls = []

    def handle(request):
        calls.append(request.content)
        return httpx.Response(429, json=REJECTION) if len(calls) == 1 else httpx.Response(200, text="result")

    pool = AdmissionPool(ClientAdmission(1))
    with ReadRetryHTTPClient(POLICY, pool, transport=httpx.MockTransport(handle)) as client:
        response = client.post(URL, content=b"{}")
    assert response.text == "result"
    assert calls == [b"{}", b"{}"]
    assert pool.stats == {"active": 0, "queued": 0}


def test_retry_forwards_remaining_body_timeout_without_reencoding_other_values():
    requests = []
    original = b'{ "large": 12345678901234567890123456789, "decimal": 1.0000000000000000001, "timeout_ms": 150 }'

    def handle(request):
        requests.append(request)
        return httpx.Response(429, json=REJECTION) if len(requests) == 1 else httpx.Response(200, json={"ok": True})

    with ReadRetryHTTPClient(ReadRetryPolicy(3, 1, 0.1, 0.1), transport=httpx.MockTransport(handle)) as client:
        assert client.post(URL, content=original).status_code == 200
    assert len(requests) == 2
    assert 0 < json.loads(requests[1].content)["timeout_ms"] < 60
    assert b'"large": 12345678901234567890123456789, "decimal": 1.0000000000000000001' in requests[1].content
    assert int(requests[1].headers["Content-Length"]) == len(requests[1].content)
    assert requests[0].extensions["antfly_deadline"] == requests[1].extensions["antfly_deadline"]


@pytest.mark.parametrize("body", [b'{ "timeout_ms": "50" }', b'{"timeout_ms":10,"timeout_ms":50}', b"not-json"])
def test_unknown_body_contract_is_forwarded_once_unchanged(body):
    seen = []

    def handle(request):
        seen.append(request.content)
        return httpx.Response(429, json=REJECTION)

    with ReadRetryHTTPClient(POLICY, transport=httpx.MockTransport(handle)) as client:
        assert client.post(URL, content=body).status_code == 429
    assert seen == [body]


@pytest.mark.asyncio
async def test_async_ndjson_uses_shortest_original_body_timeout():
    requests = []

    async def handle(request):
        requests.append(request)
        return httpx.Response(429, json=REJECTION)

    async with ReadRetryAsyncHTTPClient(
        ReadRetryPolicy(3, 1, 0.1, 0.1), transport=httpx.MockTransport(handle)
    ) as client:
        response = await client.post(
            URL, content=b'{"timeout_ms":800}\n{"timeout_ms":80}\n', headers={"Content-Type": "application/x-ndjson"}
        )
    assert response.status_code == 429
    assert len(requests) == 1
    assert all(json.loads(line)["timeout_ms"] <= 80 for line in requests[0].content.splitlines())


@pytest.mark.parametrize(
    "path,status,detail,after",
    [
        ("/db/v1/tables/docs/batch", 429, REJECTION, None),
        ("/db/v1/query", 429, {"reason": "instance_busy"}, None),
        ("/db/v1/query", 429, {**REJECTION, "execution_started": True}, None),
        ("/db/v1/query", 503, REJECTION, None),
        ("/db/v1/query", 429, REJECTION, "1"),
    ],
)
def test_no_retry_for_unsafe_or_unbounded_guidance(path, status, detail, after):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(status, json=detail, headers={"Retry-After": after} if after else {})

    with ReadRetryHTTPClient(POLICY, transport=httpx.MockTransport(handle)) as client:
        response = client.post("http://test" + path, content=b"{}")
    assert response.json() == detail
    assert len(calls) == 1


def test_attempt_limit_and_original_deadline():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(429, json=REJECTION)

    with ReadRetryHTTPClient(POLICY, transport=httpx.MockTransport(handle)) as client:
        assert client.post(URL, content=b"{}").status_code == 429
        assert len(calls) == 3
        calls.clear()
        assert (
            client.post(URL, content=b"{}", extensions={"antfly_deadline": time.monotonic() + 0.0005}).status_code
            == 429
        )
        assert len(calls) == 1


@pytest.mark.asyncio
async def test_async_cancellation_during_backoff():
    called = asyncio.Event()
    calls = []

    async def handle(request):
        calls.append(request)
        called.set()
        return httpx.Response(429, json=REJECTION)

    pool = AdmissionPool(ClientAdmission(1))
    async with ReadRetryAsyncHTTPClient(
        ReadRetryPolicy(3, 1, 0.05, 0.05), pool, transport=httpx.MockTransport(handle)
    ) as client:
        task = asyncio.create_task(client.post(URL, content=b"{}"))
        await called.wait()
        await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(calls) == 1
    assert pool.stats["active"] == 0


@pytest.mark.asyncio
async def test_async_success_and_unknown_network_outcome():
    calls = []

    async def handle(request):
        calls.append(request.content)
        return httpx.Response(429, json=REJECTION) if len(calls) == 1 else httpx.Response(200, json={"ok": True})

    async with ReadRetryAsyncHTTPClient(POLICY, transport=httpx.MockTransport(handle)) as client:
        assert (await client.post(URL, content=b"{}")).json() == {"ok": True}
    assert calls == [b"{}", b"{}"]

    async def fail(request):
        calls.append(request.content)
        raise httpx.ReadError("unknown outcome")

    calls.clear()
    async with ReadRetryAsyncHTTPClient(POLICY, transport=httpx.MockTransport(fail)) as client:
        with pytest.raises(httpx.ReadError):
            await client.post(URL, content=b"{}")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_async_stream_keeps_original_deadline_after_headers_and_backoff():
    class DelayedBody(httpx.AsyncByteStream):
        def __init__(self):
            self.closed = 0

        async def __aiter__(self):
            yield b"first"
            await asyncio.sleep(1)
            yield b"late"

        async def aclose(self):
            self.closed += 1

    body = DelayedBody()
    calls = []

    async def handle(request):
        calls.append(request)
        return httpx.Response(429, json=REJECTION) if len(calls) == 1 else httpx.Response(200, stream=body)

    pool = AdmissionPool(ClientAdmission(1))
    async with ReadRetryAsyncHTTPClient(
        ReadRetryPolicy(3, 1, 0.03, 0.03), pool, transport=httpx.MockTransport(handle)
    ) as client:
        request = client.build_request("POST", URL, content=b'{"timeout_ms":100}')
        response = await client.send(request, stream=True)
        iterator = response.aiter_raw()
        assert await anext(iterator) == b"first"
        assert pool.stats["active"] == 1
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await anext(iterator)
        # Deadline cleanup closes the actual transport and returns its admission.
        # An enclosing timeout alone would leave a broken implementation running
        # until the watchdog, so prove the original deadline was the cause.
        assert time.monotonic() < calls[0].extensions["antfly_deadline"] + 0.15
        await response.aclose()
    assert len(calls) == 2
    assert body.closed == 1
    assert pool.stats == {"active": 0, "queued": 0}


@pytest.mark.asyncio
async def test_async_stream_consumer_pause_does_not_restart_deadline():
    class ReadyBody(httpx.AsyncByteStream):
        closed = 0

        async def __aiter__(self):
            yield b"first"
            yield b"late"

        async def aclose(self):
            self.closed += 1

    body = ReadyBody()
    pool = AdmissionPool(ClientAdmission(1))
    async with ReadRetryAsyncHTTPClient(
        POLICY, pool, transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
    ) as client:
        response = await client.send(client.build_request("POST", URL, content=b'{"timeout_ms":30}'), stream=True)
        iterator = response.aiter_raw()
        assert await anext(iterator) == b"first"
        await asyncio.sleep(0.04)
        with pytest.raises(TimeoutError):
            await anext(iterator)
        await response.aclose()
    assert body.closed == 1
    assert pool.stats["active"] == 0


@pytest.mark.asyncio
async def test_async_stream_cancel_retains_permit_until_transport_closes():
    closing = asyncio.Event()
    finish_close = asyncio.Event()
    reading = asyncio.Event()

    class BlockedBody(httpx.AsyncByteStream):
        closed = 0

        async def __aiter__(self):
            reading.set()
            await asyncio.Event().wait()
            yield b"unreachable"

        async def aclose(self):
            self.closed += 1
            closing.set()
            await finish_close.wait()

    body = BlockedBody()
    pool = AdmissionPool(ClientAdmission(1))
    async with ReadRetryAsyncHTTPClient(
        POLICY, pool, transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
    ) as client:
        response = await client.send(client.build_request("POST", URL, content=b"{}"), stream=True)
        task = asyncio.create_task(response.aread())
        await reading.wait()
        task.cancel()
        await closing.wait()
        assert pool.stats["active"] == 1
        task.cancel()  # Cleanup must survive a second cancellation too.
        done, _ = await asyncio.wait({task}, timeout=0.1)
        assert task in done  # Cancellation does not wait for the transport close.
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pool.stats["active"] == 1
        finish_close.set()
        await response.aclose()
    assert body.closed == 1
    assert pool.stats == {"active": 0, "queued": 0}
