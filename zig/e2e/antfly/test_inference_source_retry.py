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

"""Failed hosted attempts release large media before retry or fallback."""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import conftest as harness
import pytest
import requests


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("mode", ["retry", "fallback"])
def test_large_source_media_survives_every_chain_attempt(tmp_path, mode):
    size = 64 * 1024 * 1024
    state = {"fetches": 0, "attempts": 0, "errors": []}

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if self.path != "/media/clip" or self.headers.get("If-Match") != '"v1"':
                self.send_error(412)
                return
            if self.headers.get("Range") != f"bytes=0-{size - 1}":
                self.send_error(416)
                return
            state["fetches"] += 1
            self.send_response(206)
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Range", f"bytes 0-{size - 1}/{size}")
            self.send_header("ETag", '"v1"')
            self.end_headers()
            chunk = bytes(64 * 1024)
            try:
                for _ in range(size // len(chunk)):
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                state["errors"].append("media fetch disconnected")

        def do_POST(self):
            remaining = int(self.headers["Content-Length"])
            # Drain without retaining another full media copy in the fixture.
            prefix = b""
            while remaining:
                chunk = self.rfile.read(min(remaining, 64 * 1024))
                if not chunk:
                    state["errors"].append("upstream request disconnected")
                    return
                if not prefix:
                    prefix = chunk[:1024]
                remaining -= len(chunk)
            if self.path != "/chat/completions" or b'"input_audio"' not in prefix:
                state["errors"].append("incorrect hosted media adaptation")
            state["attempts"] += 1
            success = state["attempts"] == 8
            result = (
                {
                    "id": "ok",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "backend",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                }
                if success
                else {"error": {"message": "retry", "type": "server_error"}}
            )
            body = json.dumps(result).encode()
            self.send_response(200 if success else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    server = None
    failed = True
    try:
        base = f"http://127.0.0.1:{fixture.server_port}"
        link = {"generator": "audio", "condition": "on_error"}
        links = (
            [
                dict(
                    link,
                    retry={
                        "max_attempts": 8,
                        "initial_backoff_ms": 1,
                        "max_backoff_ms": 1,
                    },
                )
            ]
            if mode == "retry"
            else [dict(link) for _ in range(8)]
        )
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps(
                {
                    "public_inference": {"enabled": True, "default_timeout_ms": 120000},
                    "generators": {
                        "audio": {"provider": "openai", "model": "backend", "url": base}
                    },
                    "inference_capabilities": {
                        "generators/audio": {"audio": "supported"}
                    },
                    "chains": {"media": links},
                    "remote_content": {
                        "security": {
                            "source_media_base_urls": [base + "/media/"],
                            "max_download_size_bytes": size,
                        }
                    },
                }
            )
        )
        binary = harness.resolve_binary_path(
            os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
        )
        server = harness.StandaloneAntflyServer(
            binary, "127.0.0.1", 0, config_path=config
        )
        response = requests.post(
            server.url + "/ai/v1/chat/completions",
            json={
                "model": "chains/media",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "source",
                                "source": {
                                    "type": "source",
                                    "uri": base + "/media/clip",
                                    "mime_type": "audio/wav",
                                    "version": '"v1"',
                                    "length": size,
                                },
                            }
                        ],
                    }
                ],
            },
            timeout=150,
        )
        assert response.status_code == 200, response.text
        assert response.json()["choices"][0]["message"]["content"] == "ok"
        assert state == {"fetches": 1, "attempts": 8, "errors": []}
        failed = False
    finally:
        if server is not None:
            server.stop(test_failed=failed)
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=5)
