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

"""Shared (platform) inference credentials reach background enrichment.

Antfly Cloud configures only the node-level ``inference.api_url`` and
``inference.api_key`` (a secret reference) for the shared inference pool.
Index embedders name the model and inherit both. The create-time dimension
probe and query embedding already authenticated; background enrichment
inherited the URL but not the key, so every backfill request was rejected.

The node config carries only the secret reference; the value is resolved at
use time, so rotating the stored secret reaches running enrichment workers,
and a restart rebuilds them from current runtime config rather than from
anything persisted with the index.
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, StandaloneAntflyServer, resolve_binary_path
from helpers import start_http_server, wait_until

MODEL = "shared/embedder"
DIMENSIONS = 4
API_KEY = "shared-inference-e2e-key"
ROTATED_API_KEY = "shared-inference-e2e-rotated-key"
SECRET_KEY = "antfly.antfly_inference.api_key"


class _SharedInference(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _SharedInferenceHandler)
        self.lock = threading.Lock()
        self.api_key = API_KEY
        self.embed_requests: list[str | None] = []
        self.unauthorized = 0

    def rotate(self, api_key: str) -> None:
        with self.lock:
            self.api_key = api_key
            self.embed_requests.clear()

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}/ai/v1"

    def embed_authorizations(self) -> list[str | None]:
        with self.lock:
            return list(self.embed_requests)


class _SharedInferenceHandler(BaseHTTPRequestHandler):
    server: _SharedInference

    def log_message(self, *_args) -> None:
        pass

    def _reply(self, status: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self) -> bool:
        with self.server.lock:
            if self.headers.get("Authorization") == f"Bearer {self.server.api_key}":
                return True
            self.server.unauthorized += 1
        self._reply(401, {"error": "missing or invalid API key"})
        return False

    def do_GET(self) -> None:
        if not self.path.split("?", 1)[0].endswith("/ai/v1/models"):
            self._reply(404, {"error": "not found"})
            return
        if self._authorized():
            self._reply(200, {"embedders": {MODEL: {"dimensions": DIMENSIONS}}})

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if not self.path.endswith("/ai/v1/embed"):
            self._reply(404, {"error": "not found"})
            return
        with self.server.lock:
            self.server.embed_requests.append(self.headers.get("Authorization"))
        if not self._authorized():
            return
        inputs = json.loads(body)["input"]
        count = len(inputs) if isinstance(inputs, list) else 1
        self._reply(
            200,
            {"data": [{"embedding": [0.5, 0.25, 0.125, 0.0625]} for _ in range(count)]},
        )


@pytest.fixture
def shared_inference():
    server = _SharedInference()
    thread = start_http_server(server)
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _write_secret(root: Path, value: str) -> Path:
    """Replace the projected secret file the way a Kubernetes Secret update does."""
    secrets_path = root / "secrets.json"
    staged = root / "secrets.json.tmp"
    staged.write_text(
        json.dumps({"secrets": [{"key": SECRET_KEY, "value": value}]}),
        encoding="utf-8",
    )
    staged.chmod(0o600)
    os.replace(staged, secrets_path)
    return secrets_path


def _write_config(root: Path, api_url: str) -> Path:
    secrets_path = _write_secret(root, API_KEY)
    reference = "${secret:" + SECRET_KEY + "}"
    config_path = root / "shared-inference.json"
    config_path.write_text(
        json.dumps(
            {
                "inference": {"api_url": api_url, "api_key": reference},
                "connections": {
                    "cloudaf-antfly-inference": {
                        "kind": "inference",
                        "provider": "antfly",
                        "capabilities": ["models.embed", "indexing.use"],
                        "inference": {
                            "provider": "antfly",
                            "url": api_url,
                            "api_key": reference,
                            "configured_model_types": ["embedder"],
                        },
                    }
                },
                "secrets": {
                    "sources": [
                        {"name": "kubernetes", "type": "file", "path": str(secrets_path)}
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def _coverage(api_url: str, table: str) -> dict:
    response = requests.get(f"{api_url}/tables/{table}/indexes/semantic", timeout=10)
    response.raise_for_status()
    status = response.json().get("status", {})
    return {
        "backfill_state": status.get("backfill_state"),
        "searchable_vectors": status.get("searchable_vectors"),
        "covered": (status.get("source_coverage") or {}).get("covered"),
        "failed": (status.get("source_coverage") or {}).get("failed"),
    }


def _insert(api_url: str, table: str, start: int) -> None:
    inserts = {
        f"doc{n}": {"text": f"shared inference row {n}"} for n in range(start, start + 3)
    }
    response = requests.post(
        f"{api_url}/tables/{table}/batch", json={"inserts": inserts}, timeout=30
    )
    response.raise_for_status()


def _assert_only(shared_inference: _SharedInference, api_key: str) -> None:
    authorizations = shared_inference.embed_authorizations()
    assert authorizations, "background enrichment never reached shared inference"
    assert set(authorizations) == {f"Bearer {api_key}"}
    assert shared_inference.unauthorized == 0


def _wait_covered(api_url: str, table: str, expected: int) -> dict:
    status = wait_until(
        lambda: _coverage(api_url, table),
        timeout_s=60,
        interval_s=0.5,
        ready_when=lambda s: s is not None
        and (
            (s["covered"] == expected and s["backfill_state"] == "ready")
            or (s["failed"] or 0) > 0
        ),
    )
    assert status == {
        "backfill_state": "ready",
        "searchable_vectors": expected,
        "covered": expected,
        "failed": 0,
    }
    return status


@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_default_inference_api_key_authenticates_background_enrichment(
    tmp_path, shared_inference, storage_engine
):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip("Antfly binary unavailable")
    config_path = _write_config(tmp_path, shared_inference.api_url)
    server = StandaloneAntflyServer(
        binary,
        "127.0.0.1",
        0,
        config_path=config_path,
        storage_engine=None if storage_engine == "local" else storage_engine,
    )
    try:
        api_url = server.api_url
        table = "shared_inference"
        created = requests.post(
            f"{api_url}/tables/{table}",
            json={
                "indexes": {
                    "semantic": {
                        "type": "embeddings",
                        "field": "text",
                        "dimension": DIMENSIONS,
                        "embedder": {"provider": "antfly", "model": MODEL},
                    }
                }
            },
            timeout=60,
        )
        assert created.status_code == 200, created.text

        _insert(api_url, table, 1)
        _wait_covered(api_url, table, 3)
        _assert_only(shared_inference, API_KEY)

        # Rotating the referenced secret reaches running enrichment workers.
        shared_inference.rotate(ROTATED_API_KEY)
        _write_secret(tmp_path, ROTATED_API_KEY)
        _insert(api_url, table, 4)
        _wait_covered(api_url, table, 6)
        _assert_only(shared_inference, ROTATED_API_KEY)

        # Persisted indexes take the credential from current runtime config on
        # reopen, never from state stored with the index.
        shared_inference.rotate(API_KEY)
        _write_secret(tmp_path, API_KEY)
        server.restart()
        api_url = server.api_url
        _insert(api_url, table, 7)
        _wait_covered(api_url, table, 9)
        _assert_only(shared_inference, API_KEY)
    finally:
        server.stop()
