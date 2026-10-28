"""Contention and ownership contracts for the SDK's local request gate."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import httpx
import pytest

from antfly import AntflyClient, AntflyException, ClientCapacityError
from antfly._admission import Admission


def wait_queued(client: AntflyClient, count: int) -> None:
    until = time.monotonic() + 2
    while time.monotonic() < until:
        with client._admission._condition:
            if len(client._admission._waiters) == count:
                return
        threading.Event().wait(0.001)
    pytest.fail(f"SDK did not retain exactly {count} queued requests")


def assert_drained(client: AntflyClient) -> None:
    with client._admission._condition:
        assert client._admission._active == 0
        assert not client._admission._waiters


def install(client: AntflyClient, respond) -> None:
    client._client.set_httpx_client(httpx.Client(base_url="http://test", transport=httpx.MockTransport(respond)))


def test_fifo_queue_bound_and_no_replay() -> None:
    calls = []
    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=2, max_queue_wait=2)

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={})

    install(client, respond)
    with client, ThreadPoolExecutor(max_workers=2) as workers:
        with client._stream("GET", "/held"):
            first = workers.submit(client._request, "POST", "/first", json={"write": 1})
            wait_queued(client, 1)
            second = workers.submit(client.get, "table/one", "key/two")
            wait_queued(client, 2)
            with pytest.raises(ClientCapacityError) as failure:
                client._request("POST", "/overflow", json={"write": 2})
            assert failure.value.reason == "queue_full"
            assert failure.value.request_sent is False
            assert calls == ["/held"]
        assert first.result(timeout=2) == {}
        assert second.result(timeout=2) == {}
    assert calls == ["/held", "/first", "/db/v1/tables/table/one/documents/key/two"]
    assert_drained(client)


def test_queue_consumes_original_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    import antfly._admission as admission_module
    import antfly.client as client_module

    now = [100.0]
    clock = SimpleNamespace(monotonic=lambda: now[0])
    monkeypatch.setattr(admission_module, "time", clock)
    monkeypatch.setattr(client_module, "time", clock)
    seen = []
    client = AntflyClient("http://test", timeout=10, max_concurrent_requests=1, max_queue_wait=10)

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.extensions["timeout"])
        return httpx.Response(200, json={})

    install(client, respond)
    with client, ThreadPoolExecutor(max_workers=1) as workers:
        with client._stream("GET", "/held"):
            queued = workers.submit(client._request, "GET", "/queued")
            wait_queued(client, 1)
            now[0] = 103.0
        assert queued.result(timeout=2) == {}
    assert seen == [
        dict.fromkeys(("connect", "read", "write", "pool"), 10),
        dict.fromkeys(("connect", "read", "write", "pool"), 7),
    ]
    assert_drained(client)


@pytest.mark.parametrize("stage", ["encoding", "pool_construction"])
def test_budget_expiring_during_preparation_never_sends(stage: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import antfly._admission as admission_module
    import antfly.client as client_module

    now = [100.0]
    clock = SimpleNamespace(monotonic=lambda: now[0])
    monkeypatch.setattr(admission_module, "time", clock)
    monkeypatch.setattr(client_module, "time", clock)
    client = AntflyClient("http://test", timeout=10)
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={})

    pooled = httpx.Client(base_url="http://test", transport=httpx.MockTransport(respond))

    def construct(owner):
        if stage == "pool_construction":
            now[0] = 111.0
        return pooled

    def encode():
        if stage == "encoding":
            now[0] = 111.0
        return b"{}"

    monkeypatch.setattr(type(client._client), "get_httpx_client", construct)
    try:
        with pytest.raises(ClientCapacityError, match="deadline_exceeded") as failure:
            client._request("POST", "/expired", _encode_content=encode)
        assert failure.value.request_sent is False
        assert not calls
        assert_drained(client)
    finally:
        client.close()


@pytest.mark.parametrize("deadline_first", [False, True])
def test_expired_waiter_never_sends_and_fresh_operation_succeeds(deadline_first: bool) -> None:
    calls = []
    client = AntflyClient(
        "http://test",
        timeout=0.04 if deadline_first else 2,
        max_concurrent_requests=1,
        max_queue_wait=2 if deadline_first else 0.04,
    )

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={})

    install(client, respond)
    with client:
        with client._stream("GET", "/held"):
            with pytest.raises(ClientCapacityError) as failure:
                client._request("POST", "/expired")
            assert failure.value.reason == ("deadline_exceeded" if deadline_first else "wait_timeout")
            assert failure.value.request_sent is False
            assert calls == ["/held"]
            with client._admission._condition:
                assert not client._admission._waiters
                assert client._admission._active == 1
        assert client._request("GET", "/fresh") == {}
    assert calls == ["/held", "/fresh"]
    assert_drained(client)


def test_close_wakes_queued_operations_without_sending() -> None:
    calls = []
    client = AntflyClient("http://test", timeout=20, max_concurrent_requests=1, max_queue_wait=20)

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={})

    install(client, respond)
    with ThreadPoolExecutor(max_workers=2) as workers:
        with client._stream("GET", "/held"):
            first = workers.submit(client._request, "POST", "/first")
            second = workers.submit(client._request, "POST", "/second")
            wait_queued(client, 2)
            client.close()
            for result in (first, second):
                with pytest.raises(RuntimeError, match="client has been closed"):
                    result.result(timeout=1)
            assert calls == ["/held"]
        with pytest.raises(RuntimeError, match="client has been closed"):
            client._request("GET", "/after-close")
    assert_drained(client)


@pytest.mark.parametrize("mode", ["invalid_json", "read_error", "caller_error", "transport_error", "close_error"])
def test_response_and_admission_retire_on_failure(mode: str) -> None:
    closed = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            if mode == "read_error":
                raise httpx.ReadError("fixture read failed")
            yield b"{}" if mode == "close_error" else b"not JSON"

        def close(self):
            closed.append(True)
            if mode == "close_error":
                raise RuntimeError("fixture close failed")

    def respond(request: httpx.Request) -> httpx.Response:
        if mode == "transport_error":
            raise httpx.ConnectError("fixture connect failed")
        return httpx.Response(200, stream=Body())

    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=0)
    install(client, respond)
    expected = {
        "invalid_json": AntflyException,
        "read_error": httpx.ReadError,
        "caller_error": ValueError,
        "transport_error": httpx.ConnectError,
        "close_error": RuntimeError,
    }[mode]
    with client, pytest.raises(expected):
        if mode == "caller_error":
            with client._stream("GET", "/stream"):
                with pytest.raises(ClientCapacityError):
                    client._request("GET", "/overflow")
                raise ValueError("caller failed")
        else:
            client._request("GET", "/request")
    assert closed == ([] if mode == "transport_error" else [True])
    assert_drained(client)


def test_interrupted_wait_retires_its_queue_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = Admission(1, 1, 2)

    def interrupt(timeout):
        raise KeyboardInterrupt

    with gate.acquire(time.monotonic() + 2):
        monkeypatch.setattr(gate._condition, "wait", interrupt)
        with pytest.raises(KeyboardInterrupt):
            with gate.acquire(time.monotonic() + 2):
                pytest.fail("interrupted waiter was granted")
        assert not gate._waiters
        assert gate._active == 1
    assert gate._active == 0


def test_wait_ceiling_zero_fails_fast() -> None:
    gate = Admission(1, 2, 0)
    with gate.acquire(time.monotonic() + 10):
        with pytest.raises(ClientCapacityError, match="wait_timeout"):
            with gate.acquire(time.monotonic() + 10):
                pytest.fail("full gate admitted work")
        assert not gate._waiters


def test_refused_batch_does_not_encode_its_body(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=0)
    install(client, lambda request: httpx.Response(200, json={}))

    def encode(*args):
        pytest.fail("refused batch encoded its body")

    monkeypatch.setattr(client, "_encode_write_request", encode)
    with client, client._stream("GET", "/held"), pytest.raises(ClientCapacityError):
        client.batch("records", inserts={"row": {"value": 1}})
    assert_drained(client)


def test_real_tcp_slow_body_holds_admission_and_fifo_reuses_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    body_waiting = threading.Event()
    release_body = threading.Event()
    peer_closed = threading.Event()
    lock = threading.Lock()
    paths = []
    accepted = 0

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.settimeout(4)
            nonlocal accepted
            with lock:
                accepted += 1

        def do_GET(self):
            with lock:
                paths.append(self.path)
                first = len(paths) == 1
            body = b"[]" if first else json.dumps({"key": self.path.split("?")[0].rsplit("/", 1)[-1]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.flush()
            if first:
                body_waiting.set()
                if not release_body.wait(3):
                    return
            self.wfile.write(body)
            self.wfile.flush()

        def finish(self):
            try:
                super().finish()
            finally:
                peer_closed.set()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    serving.start()
    client = AntflyClient(
        f"http://127.0.0.1:{server.server_port}",
        timeout=3,
        max_concurrent_requests=1,
        max_queued_requests=2,
        max_queue_wait=2,
    )
    try:
        with client, ThreadPoolExecutor(max_workers=3) as workers:
            first = workers.submit(client.list_tables)
            try:
                assert body_waiting.wait(2)
                second = workers.submit(client.get, "records", "second")
                wait_queued(client, 1)
                third = workers.submit(client.get, "records", "third")
                wait_queued(client, 2)
                with pytest.raises(ClientCapacityError, match="queue_full"):
                    client.batch("records", inserts={"never-sent": {"value": 1}})
                with lock:
                    assert accepted == 1
                    assert paths == ["/db/v1/tables"]
            finally:
                release_body.set()
            assert first.result(timeout=2) == []
            assert second.result(timeout=2) == {"key": "second"}
            assert third.result(timeout=2) == {"key": "third"}
            with lock:
                assert accepted == 1
                assert paths == [
                    "/db/v1/tables",
                    "/db/v1/tables/records/documents/second?consistency=read_index",
                    "/db/v1/tables/records/documents/third?consistency=read_index",
                ]
        assert peer_closed.wait(2)
        assert_drained(client)
    finally:
        release_body.set()
        client.close()
        server.shutdown()
        server.server_close()
        serving.join(2)
        assert not serving.is_alive()


@pytest.mark.parametrize(
    "options",
    [
        {"max_concurrent_requests": 0},
        {"max_concurrent_requests": True},
        {"max_queued_requests": -1},
        {"max_queued_requests": 1.5},
        {"max_queue_wait": -1},
        {"max_queue_wait": float("inf")},
        {"max_queue_wait": float("nan")},
        {"timeout": 0},
        {"timeout": float("inf")},
        {"timeout": float("nan")},
    ],
)
def test_invalid_policy_is_rejected(options) -> None:
    with pytest.raises(ValueError):
        AntflyClient("http://test", **options)


def chunk_coverage() -> dict:
    return {
        "object": "chunk.embedding.list",
        "data": [],
        "status": "complete",
        "source_fingerprint": "2" * 64,
        "source_version": "v1",
        "plan_id": "1" * 64,
        "model": "model",
        "model_identity": "3" * 64,
        "model_revision": "4" * 64,
        "task_type": "RETRIEVAL_DOCUMENT",
        "dimensions": 2,
        "policy": {},
        "start_ordinal": 0,
        "covered_through_ordinal": 0,
        "gaps": [],
    }


def invoke_chunk_embedding(client: AntflyClient, streaming: bool) -> None:
    request = {"input": "hello", "embedding": {"model": "model"}}
    if streaming:
        with client.chunk_and_embed_stream(request):
            pass
    else:
        assert client.chunk_and_embed(request).model == "model"


@pytest.mark.parametrize("streaming", [False, True])
def test_chunk_embedding_refuses_before_sending_when_database_operation_owns_capacity(streaming: bool) -> None:
    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=0)
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={})

    install(client, respond)
    with client:
        with client._stream("GET", "/held"):
            with pytest.raises(ClientCapacityError, match="queue_full") as failure:
                invoke_chunk_embedding(client, streaming)
            assert failure.value.request_sent is False
            assert calls == ["/held"]
        assert client._request("GET", "/fresh") == {}
    assert calls == ["/held", "/fresh"]
    assert_drained(client)


@pytest.mark.parametrize("streaming", [False, True])
def test_chunk_embedding_queue_consumes_original_request_budget(
    streaming: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    import antfly._admission as admission_module
    import antfly.client as client_module

    now = [100.0]
    clock = SimpleNamespace(monotonic=lambda: now[0])
    monkeypatch.setattr(admission_module, "time", clock)
    monkeypatch.setattr(client_module, "time", clock)
    client = AntflyClient("http://test", timeout=10, max_concurrent_requests=1, max_queue_wait=10)
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.extensions["timeout"]))
        if request.headers.get("accept") == "application/x-ndjson":
            return httpx.Response(200, headers={"Content-Type": "application/x-ndjson"}, content=b"")
        return httpx.Response(200, json=chunk_coverage() if request.url.path == "/ai/v1/chunk" else {})

    install(client, respond)
    with client, ThreadPoolExecutor(max_workers=1) as workers:
        with client._stream("GET", "/held"):
            queued = workers.submit(invoke_chunk_embedding, client, streaming)
            wait_queued(client, 1)
            now[0] = 103.0
        queued.result(timeout=2)
    assert seen == [
        ("/held", dict.fromkeys(("connect", "read", "write", "pool"), 10)),
        ("/ai/v1/chunk", dict.fromkeys(("connect", "read", "write", "pool"), 7)),
    ]
    assert_drained(client)


def test_chunk_embedding_stream_holds_capacity_until_physical_close_returns() -> None:
    close_started = threading.Event()
    finish_close = threading.Event()
    calls = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            pytest.fail("an unread chunk embedding stream should not be pulled")
            yield b""

        def close(self):
            close_started.set()
            assert finish_close.wait(timeout=3)

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/ai/v1/chunk":
            return httpx.Response(200, headers={"Content-Type": "application/x-ndjson"}, stream=Body())
        return httpx.Response(200, json={})

    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=0)
    install(client, respond)
    with client, ThreadPoolExecutor(max_workers=1) as workers:
        closing = workers.submit(invoke_chunk_embedding, client, True)
        try:
            assert close_started.wait(timeout=2)
            with pytest.raises(ClientCapacityError, match="queue_full"):
                client._request("GET", "/must-not-send")
            assert calls == ["/ai/v1/chunk"]
        finally:
            finish_close.set()
            closing.result(timeout=2)
        assert client._request("GET", "/fresh") == {}
    assert calls == ["/ai/v1/chunk", "/fresh"]
    assert_drained(client)


@pytest.mark.parametrize("streaming", [False, True])
def test_chunk_embedding_error_retires_admission_and_preserves_server_failure(streaming: bool) -> None:
    from antfly import InferenceAPIError

    client = AntflyClient("http://test", max_concurrent_requests=1, max_queued_requests=0)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ai/v1/chunk":
            assert client._admission._active == 1
            return httpx.Response(409, json={"error": "SourceVersionChanged", "message": "source changed"})
        return httpx.Response(200, json={})

    install(client, respond)
    with client:
        with pytest.raises(InferenceAPIError) as failure:
            invoke_chunk_embedding(client, streaming)
        assert failure.value.status_code == 409
        assert failure.value.code == "SourceVersionChanged"
        assert client._request("GET", "/fresh") == {}
    assert_drained(client)
