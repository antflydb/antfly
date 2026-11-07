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

"""Provider-backed dictation shares cleanup and preserves runtime decoding hints."""

import base64
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import conftest as harness
import pytest
import requests


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize(
    "provider",
    [
        "openai",
        "antfly",
        pytest.param(
            "apple",
            marks=pytest.mark.skipif(
                os.environ.get("ANTFLY_TEST_APPLE_SPEECH") != "1",
                reason="Requires an Apple-provider build and installed en-US Speech assets",
            ),
        ),
    ],
)
def test_named_dictation_cleanup_and_streaming(tmp_path, provider):
    state = {"recognition": [], "cleanup": [], "fail_cleanup": False}

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if self.path.endswith("/dictate"):
                value = json.loads(body)
                state["recognition"].append(value)
                assert value["stream"] is False
                assert "cleanup_model" not in value
                self.reply(
                    {
                        "object": "dictation",
                        "text": "um ant fly",
                        "transcript": {
                            "text": "um ant fly",
                            "duration_ms": 1000,
                            "segments": [],
                        },
                    }
                )
            elif self.path.endswith("/audio/transcriptions"):
                state["recognition"].append({"multipart": len(body)})
                self.reply(
                    {
                        "text": "um ant fly",
                        "duration": 1.0,
                        "language": "en",
                        "segments": [],
                    }
                )
            elif self.path.endswith("/chat/completions"):
                value = json.loads(body)
                state["cleanup"].append(value)
                if state["fail_cleanup"]:
                    self.reply({"error": {"message": "unavailable"}}, status=503)
                else:
                    self.reply(
                        {
                            "id": "cleanup",
                            "object": "chat.completion",
                            "choices": [
                                {
                                    "index": 0,
                                    "message": {
                                        "role": "assistant",
                                        "content": "Antfly.",
                                    },
                                    "finish_reason": "stop",
                                }
                            ],
                            "usage": {
                                "prompt_tokens": 12,
                                "completion_tokens": 3,
                                "total_tokens": 15,
                            },
                        }
                    )
            else:
                self.send_error(404)

        def reply(self, value, status=200):
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    server = None
    try:
        base = f"http://127.0.0.1:{fixture.server_port}"
        if provider == "apple":
            transcriber = {
                "provider": "apple",
                "language_code": "en-US",
                "timestamps": True,
                "download_assets": False,
            }
        else:
            transcriber = {
                "provider": provider,
                "model": "fixture-whisper",
                "api_key": "fixture",
            }
            transcriber["api_url" if provider == "antfly" else "base_url"] = (
                base + "/ai/v1" if provider == "antfly" else base + "/v1"
            )
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps(
                {
                    "public_inference": {"enabled": True, "default_timeout_ms": 30000},
                    "resources": {
                        "inference": {"admission": {"max_concurrent_requests": 1}}
                    },
                    "transcribers": {"speech": transcriber},
                    "generators": {
                        **(
                            {"native-cleanup": {"provider": "apple", "max_tokens": 256}}
                            if provider == "apple"
                            else {}
                        ),
                        "cleanup": {
                            "provider": "openai",
                            "model": "fixture-cleanup",
                            "url": base + "/v1",
                            "api_key": "fixture",
                        },
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
        expected_raw = "um ant fly"
        audio = b"RIFFfixture"
        if provider == "apple":
            audio = (
                Path(__file__).resolve().parents[2]
                / "lib/transcribing/src/testdata/apple-speech.wav"
            ).read_bytes()
            expected_raw = "The quick brown fox jumps over the lazy dog."
        catalog = requests.get(server.url + "/ai/v1/models", timeout=40)
        assert catalog.status_code == 200, catalog.text
        described = next(
            item
            for item in catalog.json()["data"]
            if item["id"] == "transcribers/speech"
        )
        assert described["dictation"]["live_preview"] is (provider == "antfly")
        if provider == "apple":
            assert described["dictation"]["native_status"]["speech_available"]
        request = {
            "model": "transcribers/speech",
            "audio": base64.b64encode(audio).decode(),
            "cleanup_model": "generators/cleanup",
            "dictionary": ["Antfly"],
            "context": "Slack",
            "instructions": "Keep it brief",
            "style": "formal",
        }
        response = requests.post(
            server.url + "/ai/v1/dictate", json=request, timeout=40
        )
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["text"] == "Antfly."
        assert value["transcript"]["text"] == expected_raw
        assert value["usage"]["total_tokens"] == 15
        prompt = state["cleanup"][-1]["messages"][0]["content"]
        assert "Antfly" in prompt and "Slack" in prompt and "Keep it brief" in prompt
        if provider == "antfly":
            assert state["recognition"][-1]["dictionary"] == ["Antfly"]

        before = len(state["cleanup"])
        response = requests.post(
            server.url + "/ai/v1/dictate",
            json={**request, "style": "verbatim"},
            timeout=40,
        )
        assert response.status_code == 200, response.text
        assert response.json()["text"] == expected_raw
        assert len(state["cleanup"]) == before

        response = requests.post(
            server.url + "/ai/v1/dictate", json={**request, "stream": True}, timeout=40
        )
        assert response.status_code == 200, response.text
        events = [
            json.loads(line[6:])
            for line in response.text.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"
        ]
        assert [event["type"] for event in events] == [
            "dictation.transcript",
            "dictation.delta",
            "dictation.completed",
        ]
        assert events[-1]["text"] == "Antfly."
        assert "data: [DONE]" in response.text

        # The example's persistence contracts are real database APIs, not UI mocks.
        table = server.url + "/db/v1/tables/dictation"
        response = requests.post(
            table,
            json={
                "num_shards": 1,
                "indexes": {"search": {"type": "full_text", "field": "text"}},
            },
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text
        response = requests.post(
            table + "/batch",
            json={
                "sync_level": "full_index",
                "inserts": {
                    "settings-default": {
                        "kind": "settings",
                        "configuration": {"version": 1, "dictionary": ["Antfly"]},
                    },
                    "transcript-1": {
                        "kind": "transcript",
                        "text": "Antfly dictation example",
                        "raw": "ant fly",
                    },
                },
            },
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text
        response = requests.get(table + "/documents/settings-default", timeout=40)
        assert response.status_code == 200, response.text
        assert response.json()["configuration"]["dictionary"] == ["Antfly"]
        response = requests.post(
            table + "/query",
            json={"full_text_search": {"query": "Antfly"}, "limit": 50},
            timeout=40,
        )
        assert response.status_code == 200, response.text
        assert "transcript-1" in response.text, response.text
        response = requests.post(
            table + "/batch",
            json={"deletes": ["transcript-1"], "sync_level": "full_index"},
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text

        # Apple generation readiness is independent of Speech readiness.
        if (
            provider == "apple"
            and described["dictation"]["native_status"]["generation_status"] == 0
        ):
            response = requests.post(
                server.url + "/ai/v1/dictate",
                json={
                    **request,
                    "cleanup_model": "generators/native-cleanup",
                    "style": "clean",
                },
                timeout=40,
            )
            assert response.status_code == 200, response.text
            assert (
                "fox" in response.json()["text"].lower()
                and "dog" in response.json()["text"].lower()
            )

        state["fail_cleanup"] = True
        response = requests.post(
            server.url + "/ai/v1/dictate", json={**request, "stream": True}, timeout=40
        )
        assert (
            "dictation.transcript" in response.text
            and '"type":"error"' in response.text
        )
        assert "dictation.completed" not in response.text

        if provider != "antfly":
            before = len(state["recognition"])
            response = requests.post(
                server.url + "/ai/v1/dictate",
                json={**request, "transcript_prompt": "unsupported"},
                timeout=40,
            )
            assert response.status_code == 400, response.text
            assert len(state["recognition"]) == before
    finally:
        if server is not None:
            server.stop()
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=5)
