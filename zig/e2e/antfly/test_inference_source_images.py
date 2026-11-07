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

"""Pinned images with malformed headers fail as client media on every route."""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import conftest as harness
import pytest
import requests


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize(
    "payload",
    [b"\x89PNG\r\n\x1a\n", b"\x89PNG\r\n\x1a\nnot actually a png file"],
    ids=["truncated", "missing-ihdr"],
)
def test_malformed_pinned_image_is_client_media(tmp_path, payload):
    state = {"fetches": 0, "vendor_calls": 0}

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if (
                self.path != "/media/image"
                or self.headers.get("If-Match") != '"v1"'
                or self.headers.get("Range") != f"bytes=0-{len(payload) - 1}"
            ):
                self.send_error(412)
                return
            state["fetches"] += 1
            self.send_response(206)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header(
                "Content-Range", f"bytes 0-{len(payload) - 1}/{len(payload)}"
            )
            self.send_header("ETag", '"v1"')
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            state["vendor_calls"] += 1
            self.send_error(500)

    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    server = None
    failed = True
    try:
        base = f"http://127.0.0.1:{fixture.server_port}"
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps(
                {
                    "public_inference": {"enabled": True},
                    "generators": {
                        "vision": {
                            "provider": "openai",
                            "model": "backend",
                            "url": base,
                        }
                    },
                    "inference_capabilities": {
                        "generators/vision": {"images": "supported"}
                    },
                    "remote_content": {
                        "security": {"source_media_base_urls": [base + "/media/"]}
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
        body = {
            "model": "generators/vision",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "source",
                            "source": {
                                "type": "source",
                                "uri": base + "/media/image",
                                "mime_type": "image/png",
                                "version": '"v1"',
                                "length": len(payload),
                            },
                        }
                    ],
                }
            ],
        }
        for route in ("chat/completions", "generate"):
            response = requests.post(
                server.url + "/ai/v1/" + route, json=body, timeout=30
            )
            assert response.status_code == 400, response.text
            assert response.json()["error"]["code"] == "InvalidInferenceMedia"
        response = requests.post(
            server.url + "/ai/v1/generate/batch",
            json={"requests": [{"custom_id": "bad", "body": body}]},
            timeout=30,
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["data"][0]["error"]["code"] == "InvalidInferenceMedia"
        assert result["summary"] == {"total": 1, "succeeded": 0, "failed": 1}
        assert state == {"fetches": 3, "vendor_calls": 0}
        failed = False
    finally:
        if server is not None:
            server.stop(test_failed=failed)
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=5)
