"""Explicit ownership of the SDK's reusable HTTP transport."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from antfly import AntflyClient


class CountingTransport(httpx.MockTransport):
    def __init__(self) -> None:
        super().__init__(self.respond)
        self.requests = 0
        self.closes = 0

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        return httpx.Response(200, json={"ok": True}, request=request)

    def close(self) -> None:
        self.closes += 1
        super().close()


def make_client(token: str | None) -> tuple[AntflyClient, httpx.Client, CountingTransport]:
    client = AntflyClient("http://test", token=token)
    transport = CountingTransport()
    pooled = httpx.Client(base_url="http://test", transport=transport)
    client._client.set_httpx_client(pooled)
    return client, pooled, transport


@pytest.mark.parametrize("token", [None, "service-token"])
def test_close_releases_pool_once_and_does_not_reopen(token: str | None) -> None:
    client, pooled, transport = make_client(token)
    assert client._request("GET", "/health") == {"ok": True}
    client.close()
    client.close()
    assert pooled.is_closed
    assert transport.closes == 1
    with pytest.raises(RuntimeError, match="client has been closed"):
        client._request("GET", "/health")
    assert transport.requests == 1
    assert client._client.get_httpx_client() is pooled


@pytest.mark.parametrize("token", [None, "service-token"])
@pytest.mark.parametrize("fail", [False, True])
def test_context_reuses_pool_and_closes_on_exit(token: str | None, fail: bool) -> None:
    client, pooled, transport = make_client(token)

    def run() -> None:
        with client as entered:
            assert entered is client
            for _ in range(2):
                assert entered._request("GET", "/health") == {"ok": True}
                assert entered._client.get_httpx_client() is pooled
            assert not pooled.is_closed
            if fail:
                raise ValueError("caller failed")

    if fail:
        with pytest.raises(ValueError, match="caller failed"):
            run()
    else:
        run()
    assert pooled.is_closed
    assert transport.requests == 2
    assert transport.closes == 1
    client.close()
    assert transport.closes == 1


@pytest.mark.parametrize("token", [None, "service-token"])
def test_default_pool_reuses_tcp_connection_and_context_closes_peer(
    token: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    accepted = 0
    requests = 0
    lock = threading.Lock()
    peer_closed = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(5)
            nonlocal accepted
            with lock:
                accepted += 1

        def do_GET(self) -> None:
            nonlocal requests
            with lock:
                requests += 1
            if self.path != "/db/v1/tables":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"[]")

        def finish(self) -> None:
            try:
                super().finish()
            finally:
                peer_closed.set()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    serving.start()
    try:
        with AntflyClient(f"http://127.0.0.1:{server.server_port}", token=token, timeout=2) as client:
            assert client.list_tables() == []
            assert client.list_tables() == []
            with lock:
                assert accepted == 1
                assert requests == 2
            assert not peer_closed.is_set()
        # Observe EOF before stopping the server: cleanup must come from the
        # public SDK owner, rather than fixture shutdown closing the socket.
        assert peer_closed.wait(2)
    finally:
        server.shutdown()
        server.server_close()
        serving.join(2)
        assert not serving.is_alive()
