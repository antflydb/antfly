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

import base64
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
                        {
                            "name": "kubernetes",
                            "type": "file",
                            "path": str(secrets_path),
                        }
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
        f"doc{n}": {"text": f"shared inference row {n}"}
        for n in range(start, start + 3)
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
        ready_when=lambda s: (
            s is not None
            and (
                (s["covered"] == expected and s["backfill_state"] == "ready")
                or (s["failed"] or 0) > 0
            )
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
@pytest.mark.e2e_resource("antfly_process")
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


# --- Remote task families --------------------------------------------------
#
# Every remote task family that inherits the node default endpoint inherits
# its credential too: in-table chunkers, readers, generators, extractors and
# transcribers (storage-owned background work) and query-time rerankers. An
# index or producer that names its own endpoint never receives it.

TASK_MODELS = {
    "chunkers": "shared/chunker",
    "rerankers": "shared/reranker",
    "readers": "shared/reader",
    "generators": "shared/generator",
    "extractors": "shared/extractor",
    "transcribers": "shared/transcriber",
}
ZIG_ROOT = Path(__file__).resolve().parents[2]


def _data_uri(mime_type: str, relative: str) -> str:
    raw = (ZIG_ROOT / relative).read_bytes()
    return f"data:{mime_type};base64," + base64.b64encode(raw).decode("ascii")


class _TaskInference(ThreadingHTTPServer):
    """Antfly inference stand-in that records Authorization per task."""

    daemon_threads = True

    def __init__(self, api_key: str, enforce: bool) -> None:
        super().__init__(("127.0.0.1", 0), _TaskInferenceHandler)
        self.lock = threading.Lock()
        self.api_key = api_key
        self.enforce = enforce
        self.seen: list[tuple[str, str | None]] = []

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}/ai/v1"

    def tasks(self) -> set[str]:
        with self.lock:
            return {task for task, _ in self.seen}

    def authorizations(self) -> set[str | None]:
        with self.lock:
            return {authorization for _, authorization in self.seen}

    def requests(self) -> list[tuple[str, str | None]]:
        with self.lock:
            return list(self.seen)


class _TaskInferenceHandler(BaseHTTPRequestHandler):
    server: _TaskInference

    def log_message(self, *_args) -> None:
        pass

    def _reply(self, status: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _admit(self, task: str) -> bool:
        authorization = self.headers.get("Authorization")
        with self.server.lock:
            self.server.seen.append((task, authorization))
        if self.server.enforce and authorization != f"Bearer {self.server.api_key}":
            self._reply(401, {"error": "missing or invalid API key"})
            return False
        return True

    def do_GET(self) -> None:
        if not self.path.split("?", 1)[0].endswith("/ai/v1/models"):
            self._reply(404, {"error": "not found"})
            return
        if self._admit("models"):
            catalog = {
                family: {model: {"inputs": ["text", "image", "audio"]}}
                for family, model in TASK_MODELS.items()
            }
            catalog["embedders"] = {MODEL: {"dimensions": DIMENSIONS}}
            self._reply(200, catalog)

    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        task = self.path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        if task not in (
            "embed",
            "chunk",
            "rerank",
            "read",
            "generate",
            "extract",
            "transcribe",
        ):
            self._reply(404, {"error": "not found"})
            return
        if not self._admit(task):
            return
        try:
            body = json.loads(raw or b"{}")
        except ValueError:
            body = {}
        if task == "embed":
            inputs = body.get("input")
            count = len(inputs) if isinstance(inputs, list) else 1
            self._reply(
                200, {"data": [{"embedding": [0.5, 0.25, 0.125, 0.0625]}] * count}
            )
        elif task == "chunk":
            text = str(body.get("input", ""))
            self._reply(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": 0,
                            "mime_type": "text/plain",
                            "text": text,
                            "start_char": 0,
                            "end_char": len(text),
                        }
                    ],
                },
            )
        elif task == "rerank":
            count = len(body.get("documents") or body.get("prompts") or [])
            self._reply(200, {"scores": [1.0 - i / 100 for i in range(count)]})
        elif task == "read":
            count = max(1, len(body.get("images") or []))
            self._reply(
                200,
                {
                    "object": "list",
                    "data": [
                        {"object": "read_result", "index": i, "text": "read text"}
                        for i in range(count)
                    ],
                },
            )
        elif task == "generate":
            self._reply(
                200,
                {
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "generated"},
                            "finish_reason": "stop",
                        }
                    ]
                },
            )
        else:
            self._reply(200, {"text": "", "data": []})


def _task_server(api_key: str, enforce: bool):
    server = _TaskInference(api_key, enforce)
    return server, start_http_server(server)


# Families that must reach the shared endpoint from background enrichment
# (storage-owned): model chunking and document-extraction OCR. Generator,
# extractor, transcriber and standalone reader producers are covered by unit
# tests only: against this stand-in a remote generator or transcriber producer
# aborts the server after its first response, and Antfly extractor and reader
# producers send no request; these reproduce on main with an explicit
# producer url, independent of credentials.
BACKGROUND_TASKS = {"chunk", "read"}


@pytest.mark.parametrize("storage_engine", ["local", "lite"])
@pytest.mark.e2e_resource("antfly_process")
def test_default_inference_api_key_reaches_remote_tasks_only_at_default_endpoint(
    tmp_path, storage_engine
):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip("Antfly binary unavailable")
    shared, shared_thread = _task_server(API_KEY, enforce=False)
    third_party, third_thread = _task_server("third-party-key", enforce=False)
    config_path = _write_config(tmp_path, shared.api_url)
    server = StandaloneAntflyServer(
        binary,
        "127.0.0.1",
        0,
        config_path=config_path,
        storage_engine=None if storage_engine == "local" else storage_engine,
    )
    try:
        api_url = server.api_url
        table = "remote_tasks"
        embedder = {"provider": "antfly", "model": MODEL}
        created = requests.post(
            f"{api_url}/tables/{table}",
            json={
                "indexes": {
                    "chunked": {
                        "type": "embeddings",
                        "field": "text",
                        "dimension": DIMENSIONS,
                        "embedder": embedder,
                        "chunker": {
                            "provider": "antfly",
                            "model": TASK_MODELS["chunkers"],
                            "text": {"target_tokens": 8},
                        },
                    },
                    # Names its own endpoint and no key: never sent the node key.
                    "third_party": {
                        "type": "embeddings",
                        "field": "text",
                        "dimension": DIMENSIONS,
                        "embedder": {**embedder, "api_url": third_party.api_url},
                    },
                }
            },
            timeout=60,
        )
        assert created.status_code == 200, created.text
        # Document extraction with OCR: the reader inherits the default
        # endpoint and must inherit its credential.
        extraction = {
            "type": "document_extraction",
            "config": {
                "source": {
                    "filename_field": "filename",
                    "content_type_field": "mime_type",
                    "version_field": "version",
                },
                "ocr": {
                    "mode": "always",
                    "reader": {"provider": "antfly", "model": TASK_MODELS["readers"]},
                },
            },
        }
        response = requests.put(
            f"{api_url}/tables/{table}/artifacts/document_units/enrichment",
            json={
                "kind": "asset",
                "field": "img",
                "content_type": "application/json",
                "producer_json": json.dumps(extraction),
            },
            timeout=30,
        )
        assert response.status_code in (200, 201), response.text

        def insert(start: int) -> None:
            rows = {
                f"doc{n}": {
                    "text": f"Ada Lovelace wrote shared inference row {n}",
                    "img": _data_uri(
                        "image/jpeg", "testdata/image/jpeg/baseline/white-2x1.jpg"
                    ),
                    "filename": f"page{n}.jpg",
                    "mime_type": "image/jpeg",
                    "version": "1",
                }
                for n in range(start, start + 2)
            }
            response = requests.post(
                f"{api_url}/tables/{table}/batch", json={"inserts": rows}, timeout=30
            )
            response.raise_for_status()

        def background_reached(
            server_obj: _TaskInference, wanted: set[str], since: int = 0
        ) -> None:
            seen = wait_until(
                lambda: {task for task, _ in server_obj.requests()[since:]},
                timeout_s=60,
                interval_s=0.5,
                ready_when=lambda tasks: tasks is not None and wanted <= tasks,
            )
            assert seen is not None, (wanted, server_obj.requests()[since:])

        insert(1)
        background_reached(shared, BACKGROUND_TASKS)
        background_reached(third_party, {"embed"})

        reranked = requests.post(
            f"{api_url}/tables/{table}/query",
            json={
                "full_text_search": {"query": "Lovelace"},
                "limit": 2,
                "reranker": {
                    "provider": "antfly",
                    "model": TASK_MODELS["rerankers"],
                    "field": "text",
                },
            },
            timeout=30,
        )
        assert reranked.status_code == 200, reranked.text
        assert "rerank" in shared.tasks()

        # Reopen: storage-owned producers are rebuilt from runtime config.
        server.restart()
        api_url = server.api_url
        before = len(shared.requests())
        insert(3)
        background_reached(shared, BACKGROUND_TASKS, before)

        assert shared.authorizations() == {f"Bearer {API_KEY}"}, shared.requests()
        assert third_party.authorizations() == {None}, third_party.requests()
    finally:
        server.stop()
        for srv, thread in ((shared, shared_thread), (third_party, third_thread)):
            srv.shutdown()
            srv.server_close()
            thread.join(timeout=5)
