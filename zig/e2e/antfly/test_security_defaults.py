# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Security boundaries exercised through the real public HTTP API."""

from __future__ import annotations

import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import test_auth as auth
from conftest import (
    DEFAULT_ANTFLY_BIN,
    _standalone_stateful_command,
    resolve_binary_path,
)
from helpers import start_http_server, wait_until

auth_api = auth.auth_api


def test_mcp_session_does_not_replace_request_authentication(auth_api):
    root = auth_api.auth_url.removesuffix(auth.AUTH_PUBLIC_API_ROOT)
    auth_api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    initialized = auth_api.s.post(
        f"{root}/mcp/v1",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "security-test", "version": "1"},
            },
        },
        timeout=30,
    )
    initialized.raise_for_status()
    session_headers = {
        "Mcp-Session-Id": initialized.headers["Mcp-Session-Id"],
        "Mcp-Protocol-Version": "2025-06-18",
    }
    del auth_api.s.headers["Authorization"]
    table = f"mcp_denied_{time.time_ns()}"
    response = auth_api.s.post(
        f"{root}/mcp/v1",
        headers=session_headers,
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "create_table",
                "arguments": {"tableName": table, "numShards": 1},
            },
        },
        timeout=30,
    )
    assert response.status_code in (401, 403), response.text
    auth_api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    assert all(entry["name"] != table for entry in auth_api.get("/tables"))


def test_fresh_authenticated_server_requires_bootstrap_password(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")
    env = os.environ.copy()
    env.pop("ANTFLY_BOOTSTRAP_ADMIN_PASSWORD", None)
    command = _standalone_stateful_command(
        binary, host="127.0.0.1", port=0, root=tmp_path
    )
    command.extend(["--auth", "true"])
    result = subprocess.run(
        command, env=env, capture_output=True, text=True, timeout=30, check=False
    )
    assert result.returncode != 0
    assert "ANTFLY_BOOTSTRAP_ADMIN_PASSWORD" in result.stderr


def test_local_file_enrichment_is_denied_before_provider_dispatch(backup_api, tmp_path):
    marker = "private-file-marker-927"
    local_file = tmp_path / "private.txt"
    local_file.write_text(marker)
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            inputs = payload.get("input", [])
            if isinstance(inputs, str):
                inputs = [inputs]
            received.extend(inputs)
            body = json.dumps(
                {
                    "object": "list",
                    "data": [
                        {
                            "object": "embedding",
                            "index": i,
                            "embedding": [1.0, 0.0, 0.0],
                        }
                        for i, _ in enumerate(inputs)
                    ],
                    "model": "test",
                    "usage": {"prompt_tokens": 1, "total_tokens": 1},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    provider = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(provider)
    table = f"file_security_{time.time_ns()}"
    try:
        backup_api.create_table(table, num_shards=1)
        embedder = {
            "provider": "openai",
            "model": "test",
            "url": f"http://127.0.0.1:{provider.server_port}",
        }
        backup_api.create_index(
            table,
            "control",
            {
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": embedder,
            },
        )
        backup_api.create_index(
            table,
            "file",
            {
                "type": "embeddings",
                "template": "{{remoteText url=source}}",
                "dimension": 3,
                "embedder": embedder,
            },
        )
        backup_api.batch_write(
            table,
            inserts={
                "doc": {"body": "safe-control-content", "source": local_file.as_uri()}
            },
        )
        assert wait_until(
            lambda: "safe-control-content" in received, timeout_s=30, interval_s=0.1
        )

        def rejected():
            detail = backup_api.get_index(table, "file")
            runtime = detail.get("status", {}).get("enrichment_runtime", {})
            return detail if int(runtime.get("fatal_error_count", 0)) > 0 else None

        failure = wait_until(rejected, timeout_s=30, interval_s=0.1)
        assert failure is not None, backup_api.get_index(table, "file")
        # Query templates use a different production entry point from
        # background enrichment, including request deadline propagation.
        backup_api.wait_index_ready(table, "control", until="complete")
        control = backup_api.query_table(
            table,
            {
                "semantic_search": "safe-query-content",
                "embedding_template": "{{this}}",
                "indexes": ["control"],
                "limit": 1,
            },
        )
        assert control is not None
        assert "safe-query-content" in received, received
        response = backup_api.s.post(
            f"{backup_api.url}/tables/{table}/query",
            json={
                "semantic_search": local_file.as_uri(),
                "embedding_template": "{{remoteText url=this}}",
                "indexes": ["control"],
                "limit": 1,
            },
            timeout=30,
        )
        assert response.status_code == 400, response.text
        assert response.text == "invalid query request", response.text
        assert marker not in received, received
    finally:
        provider.shutdown()
        provider.server_close()
        thread.join(timeout=5)
