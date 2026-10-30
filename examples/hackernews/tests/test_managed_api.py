# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from managed_api import authorization_headers, open_request, validate_backend
from serve import Gateway, RateLimit


def test_backend_and_secret_validation_does_not_reveal_values():
    base = "https://cloud.antfly.io/cloud/v1/instance/db/v1"
    assert validate_backend(base + "/", True) == base
    assert validate_backend("http://127.0.0.1:8877/db/v1")
    for url in (
        "http://127.0.0.1/db/v1",
        "http://remote/db/v1",
        "https://user:secret@host/db/v1",
        "https://host/db/v1?key=secret",
        "https://host/db/v1#secret",
        "https://host/a/%2e%2e/db/v1",
        "https://host:invalid/db/v1",
        "https://host/db/v1\n",
    ):
        with pytest.raises(ValueError) as error:
            validate_backend(url, True)
        assert "secret" not in str(error.value)
    assert authorization_headers({"ANTFLY_AUTHORIZATION": "Bearer read-only"}) == {
        "Authorization": "Bearer read-only"
    }
    assert authorization_headers({"ANTFLY_API_KEY": "key"}) == {
        "Authorization": "Bearer key"
    }
    for env in (
        {"ANTFLY_API_KEY": "a", "ANTFLY_AUTHORIZATION": "Bearer b"},
        {"ANTFLY_AUTHORIZATION": "Bearer secret\r\nInjected: value"},
        {"ANTFLY_AUTHORIZATION": "Bearer "},
    ):
        with pytest.raises(ValueError) as error:
            authorization_headers(env)
        assert "secret" not in str(error.value)


def test_gateway_uses_server_credential_for_readiness_and_search_and_bounds_rate():
    requests = []

    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            requests.append((self.path, self.headers.get("Authorization")))
            body = {"status": {"readiness": {"queryable": True}}}
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, self.headers.get("Authorization")))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"responses":[{"hits":{"hits":[]}}]}')

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    gateway = ThreadingHTTPServer(("127.0.0.1", 0), Gateway)
    # The production configurator forbids credentials over HTTP. This local
    # transport fixture bypasses configuration only to inspect wire headers.
    gateway.backend = f"http://127.0.0.1:{backend.server_port}/db/v1"
    gateway.backend_headers = {"Authorization": "Bearer server-only"}
    gateway.searches = threading.BoundedSemaphore(1)
    gateway.rate_limit = RateLimit(0.00001, 1)
    gateway.ready_lock = threading.Lock()
    gateway.ready_until, gateway.ready_value = 0.0, False
    threads = [
        threading.Thread(target=s.serve_forever, daemon=True)
        for s in (backend, gateway)
    ]
    for t in threads:
        t.start()
    try:
        root = f"http://127.0.0.1:{gateway.server_port}"
        with urlopen(root + "/readyz") as response:
            assert json.load(response) == {"ready": True}
        with urlopen(root + "/readyz") as response:
            assert json.load(response) == {"ready": True}
        assert len(requests) == 3
        with urlopen(
            Request(
                root + "/api/search",
                b'{"q":"database"}',
                {"Authorization": "Bearer visitor"},
            )
        ) as response:
            assert b"server-only" not in response.read()
        with pytest.raises(HTTPError) as error:
            urlopen(Request(root + "/api/search", b'{"q":"database"}'))
        assert error.value.code == 429
        assert len(requests) == 4
        assert all(value == "Bearer server-only" for _, value in requests)
    finally:
        for s in (gateway, backend):
            s.shutdown()
            s.server_close()
        for t in threads:
            t.join()


def test_redirect_does_not_forward_credentials_or_replay_write():
    seen = []

    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            seen.append(self.path)
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/target")
            else:
                self.send_response(200)
            self.end_headers()

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    thread = threading.Thread(target=backend.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(HTTPError) as error:
            open_request(
                Request(
                    f"http://127.0.0.1:{backend.server_port}/redirect",
                    headers={"Authorization": "Bearer fixture"},
                ),
                timeout=2,
            )
        assert error.value.code == 302
        assert seen == ["/redirect"]
    finally:
        backend.shutdown()
        backend.server_close()
        thread.join()


def test_managed_client_identifies_itself_and_preserves_explicit_agent():
    seen = []

    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            seen.append(self.headers.get("User-Agent"))
            self.send_response(200)
            self.end_headers()

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    thread = threading.Thread(target=backend.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{backend.server_port}/"
        for request in (url, Request(url, headers={"User-Agent": "custom-client/2"})):
            with open_request(request, timeout=2) as response:
                assert response.status == 200
        assert seen == ["antfly-hackernews/1", "custom-client/2"]
    finally:
        backend.shutdown()
        backend.server_close()
        thread.join()
