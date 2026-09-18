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
