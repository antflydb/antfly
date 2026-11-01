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

"""Public attachment framing through a real gateway and synthetic remote provider."""

import base64
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import pytest
import requests
from e2e_scheduler import e2e_resource

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII="
)
MIME = b"image/png"


@pytest.fixture
@e2e_resource("antfly_process")
def segmented_gateway(monkeypatch, request):
    import conftest

    seen = []
    state = {"status": 200}

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            seen.append((self.path, self.headers["Content-Type"], raw))
            metadata_size, count, reserved = struct.unpack_from("<QII", raw, 8)
            assert raw[:8] == b"AFATT001" and reserved == 0 and count == 2
            offset = 24 + 16 * count
            metadata = json.loads(raw[offset : offset + metadata_size])
            offset += metadata_size
            assert metadata["model"] == "fixture"
            for i in range(count):
                mime_size, flags, size = struct.unpack_from("<IIQ", raw, 24 + 16 * i)
                assert flags == 0 and raw[offset : offset + mime_size] == MIME
                offset += mime_size
                assert raw[offset : offset + size] == PNG
                offset += size
                assert (
                    metadata["messages"][0]["content"][i]["data"] == f"attachment:{i}"
                )
            assert offset == len(raw)
            streaming = metadata["stream"]
            payload = (
                b'data: {"choices":[{"delta":{"content":"fixture"}}]}\n\ndata: [DONE]\n\n'
                if streaming
                else b'{"choices":[{"message":{"role":"assistant","content":"fixture"}}]}'
            )
            self.send_response(state["status"])
            self.send_header(
                "Content-Type", "text/event-stream" if streaming else "application/json"
            )
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    provider = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    original = conftest._write_remote_content_e2e_config

    def configure(*args, **kwargs):
        path = original(*args, **kwargs)
        cfg = json.loads(path.read_text())
        cfg.setdefault("connections", {})["segmented"] = {
            "kind": "inference",
            "capabilities": ["models.generate"],
            "inference": {
                "provider": "antfly",
                "url": f"http://127.0.0.1:{provider.server_port}/ai/v1",
            },
        }
        cfg.setdefault("inference_capabilities", {})[
            "connections/segmented/models/fixture"
        ] = {"images": "supported", "streaming": "supported"}
        path.write_text(json.dumps(cfg))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configure)
    try:
        api = request.getfixturevalue("stateful_api")
        yield api, seen, state
    finally:
        provider.shutdown()
        provider.server_close()
        thread.join(timeout=5)


@pytest.mark.fresh_antfly_process
@pytest.mark.parametrize("streaming,status", [(False, 200), (True, 200), (False, 503)])
def test_remote_segmented_generation_preserves_public_frame(
    segmented_gateway, streaming, status
):
    api, seen, state = segmented_gateway
    state["status"] = status
    metadata = json.dumps(
        {
            "model": "connections/segmented/models/fixture",
            "stream": streaming,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "media",
                            "mime_type": "image/png",
                            "data": f"attachment:{i}",
                        }
                        for i in range(2)
                    ],
                }
            ],
        }
    ).encode()
    frame = (
        b"AFATT001"
        + struct.pack("<QII", len(metadata), 2, 0)
        + struct.pack("<IIQ", len(MIME), 0, len(PNG)) * 2
        + metadata
        + (MIME + PNG) * 2
    )
    origin = urlsplit(api.url)
    response = requests.post(
        f"{origin.scheme}://{origin.netloc}/ai/v1/generate",
        data=frame,
        headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
        timeout=30,
    )
    assert response.status_code == status, (response.text, api.debug_logs()[-6000:])
    assert len(seen) == 1
    assert seen[0][0] == "/ai/v1/generate"
    assert seen[0][1] == "application/vnd.antfly.attachments.v1"
    assert b"fixture" in response.content
