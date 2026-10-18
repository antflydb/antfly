#!/usr/bin/env python3
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

"""Exercise a remote-only OSS gateway with the official OpenAI SDK.

Run with: uv run --with openai==3.35.1 python scripts/ci/zig-inference-gateway-conformance.py
All upstreams and credentials are local fixtures. No provider account is needed.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import openai


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    calls: ClassVar[list[str]] = []
    chat_attempts: ClassVar[list[str]] = []
    requests: ClassVar[list[tuple[str, dict]]] = []
    authorizations: ClassVar[list[str | None]] = []

    def log_message(self, *_args):
        pass

    def reply(self, value, status=200, content_type="application/json"):
        encoded = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/ai/v1/models":
            self.reply(
                {
                    "object": "list",
                    "data": [{"id": "mock-chat", "object": "model"}],
                    "generators": {"mock-chat": {}},
                    "embedders": {"mock-sparse": {}, "mock-multimodal": {}},
                    "rerankers": {"mock-rerank": {}},
                }
            )
        elif path == "/ml/v1/models":
            self.reply(
                {
                    "object": "list",
                    "predictors": {
                        "mock-risk": {
                            "task": "regression",
                            "num_features": 2,
                            "num_outputs": 1,
                            "feature_names": ["first", "second"],
                        }
                    },
                }
            )
        elif path.endswith("/events"):
            self.reply(
                b'data: {"type":"session.open","session_id":"backend-session"}\n\ndata: [DONE]\n\n',
                content_type="text/event-stream",
            )
        elif path == "/ai/v1/transcription/sessions/backend-session":
            self.reply(
                {
                    "id": "backend-session",
                    "model": "mock-transcriber",
                    "object": "transcription.session",
                }
            )
        else:
            self.reply({"error": "missing"}, 404)

    def do_DELETE(self):
        assert self.path == "/ai/v1/transcription/sessions/backend-session"
        self.authorizations.append(self.headers.get("Authorization"))
        assert self.headers.get("Authorization") in (
            "Bearer mock-server-key",
            "Bearer speech-api-key",
            "Bearer speech-bearer-key",
        )
        self.reply(b"", 204)

    def extra(self, request):
        path = self.path
        if path == "/ai/v1/generate":
            assert request["model"] == "mock-chat"
            self.calls.append(path)
            self.reply(
                {
                    "id": "mock-generate",
                    "object": "chat.completion",
                    "created": 1,
                    "model": request["model"],
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "hello"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 2,
                        "completion_tokens": 1,
                        "total_tokens": 3,
                    },
                }
            )
            return True
        if path == "/ai/v1/chat/completions":
            self.chat_attempts.append(request["model"])
            if request["model"] in ("mock-busy", "mock-unauthorized"):
                self.calls.append(path)
                self.reply(
                    {"error": {"message": "controlled failure"}},
                    503 if request["model"] == "mock-busy" else 401,
                )
                return True
            return False
        responses = {
            "/ai/v1/rerank": {
                "object": "list",
                "model": "mock-rerank",
                "data": [{"object": "rerank", "index": 0, "score": 0.9}],
            },
            "/ai/v1/chunk": {
                "object": "list",
                "model": "fixed",
                "data": [
                    {
                        "object": "chunk",
                        "index": 0,
                        "id": 0,
                        "text": "hello",
                        "mime_type": "text/plain",
                        "start_char": 0,
                        "end_char": 5,
                    }
                ],
            },
            "/ai/v1/rewrite": {
                "object": "list",
                "model": "mock-rewriter",
                "data": [{"object": "rewrite", "index": 0, "texts": ["rewritten"]}],
            },
            "/ai/v1/extract": {
                "schema_version": 2,
                "data": [
                    {"index": 0, "entities": [{"text": "Ada", "start": 0, "end": 3}]}
                ],
            },
            "/ai/v1/transcribe": {
                "object": "list",
                "model": "mock-transcriber",
                "data": [
                    {
                        "object": "transcription",
                        "index": 0,
                        "text": "hello",
                        "segments": [{"text": "hello", "start_ms": 10, "end_ms": 20}],
                    }
                ],
            },
            "/ai/v1/dictate": {
                "object": "dictation",
                "model": "mock-transcriber",
                "text": "hello",
                "transcript": {"text": "hello"},
            },
            "/ml/v1/predict": {
                "model": "mock-risk",
                "task": "regression",
                "predictions": [[0.5]],
            },
            "/ai/v1/transcription/sessions": {
                "id": "backend-session",
                "model": "mock-transcriber",
                "object": "transcription.session",
            },
            "/ai/v1/transcription/sessions/backend-session/audio": {
                "object": "list",
                "session_id": "backend-session",
                "data": [],
            },
            "/ai/v1/embed": {
                "object": "list",
                "model": "mock-sparse",
                "data": [
                    {
                        "object": "embedding",
                        "index": 0,
                        "embedding": {"indices": [1, 9], "values": [0.25, 0.5]},
                    }
                ],
            },
        }
        if path == "/v1/audio/speech":
            assert request["model"] == "mock-voice"
            self.calls.append(path)
            self.reply(b"ID3mock-audio", content_type="audio/mpeg")
            return True
        if path in responses:
            self.calls.append(path)
            if path == "/ai/v1/chunk":
                assert request["config"]["model"] == "fixed"
            self.reply(responses[path])
            return True
        return False

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append((self.path, request))
        self.authorizations.append(self.headers.get("Authorization"))
        assert self.headers.get("Authorization") in (
            "Bearer mock-server-key",
            "Bearer speech-api-key",
            "Bearer speech-bearer-key",
        )
        if self.extra(request):
            return
        expected = {
            "/ai/v1/chat/completions": "mock-chat",
            "/ai/v1/decisions": "mock-decision",
            "/v1/embeddings": "mock-embedding",
            "/ai/v1/read": "mock-reader",
        }
        assert request["model"] == expected[self.path], (self.path, request)
        self.calls.append(self.path)
        if self.path == "/ai/v1/chat/completions":
            if request.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                for index, arguments in enumerate(('{"query":', '"hello"}')):
                    chunk = {
                        "id": "mock-stream",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": request["model"],
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            **(
                                                {"id": "call_1", "type": "function"}
                                                if index == 0
                                                else {}
                                            ),
                                            "function": {
                                                **(
                                                    {"name": "lookup"}
                                                    if index == 0
                                                    else {}
                                                ),
                                                "arguments": arguments,
                                            },
                                        }
                                    ]
                                },
                                "finish_reason": None,
                            }
                        ],
                    }
                    self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(0.05)
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                self.close_connection = True
                return
            response = {
                "id": "mock-chat",
                "object": "chat.completion",
                "created": 1,
                "model": request["model"],
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "hello"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 1,
                    "total_tokens": 3,
                },
            }
        elif self.path == "/ai/v1/decisions":
            response = {
                "id": "mock-decision",
                "object": "decision",
                "created_at": 1,
                "model": request["model"],
                "answers": [
                    {
                        "name": request["questions"][0]["name"],
                        "type": "predicate",
                        "decision_method": "typed",
                        "probability": 0.9,
                    }
                ],
                "usage": {"input_tokens": 2, "output_tokens": 0, "total_tokens": 2},
            }
        elif self.path == "/v1/embeddings":
            response = {
                "object": "list",
                "model": request["model"],
                "data": [{"object": "embedding", "index": 0, "embedding": [0.25, 0.5]}],
                "usage": {"prompt_tokens": 1, "total_tokens": 1},
            }
        else:
            response = {
                "object": "list",
                "model": request["model"],
                "data": [{"object": "read", "index": 0, "text": "recognized"}],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 1,
                    "total_tokens": 3,
                },
            }
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--antfly",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "zig/zig-out/bin/antfly",
    )
    args = parser.parse_args()
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(
            prefix="antfly-gateway-conformance-"
        ) as temporary:
            root = Path(temporary)
            upstream_url = f"http://127.0.0.1:{upstream.server_port}"
            config = {
                "inference": {
                    "api_url": upstream_url,
                    "api_key": "mock-server-key",
                    "models_dir": str(root / "no-model-files"),
                },
                "inference_capabilities": {
                    "generators/support": {
                        "streaming": "supported",
                        "tools": "supported",
                    }
                },
                "generators": {"support": {"provider": "antfly", "model": "mock-chat"}},
                "deciders": {
                    "triage": {"provider": "antfly", "model": "mock-decision"}
                },
                "embedders": {
                    "dense": {
                        "provider": "openai",
                        "model": "mock-embedding",
                        "url": upstream_url,
                        "api_key": "mock-server-key",
                        "dimensions": 2,
                    }
                },
                "readers": {"ocr": {"provider": "antfly", "model": "mock-reader"}},
                "chains": {
                    "fallback": [
                        {"generator": "busy", "condition": "on_error"},
                        {"generator": "support"},
                    ],
                    "mixed": [
                        {"generator": "busy", "condition": "on_error"},
                        {"generator": "external"},
                    ],
                    "no-auth-fallback": [
                        {"generator": "unauthorized", "condition": "on_error"},
                        {"generator": "support"},
                    ],
                },
                "chunkers": {
                    "paragraphs": {
                        "provider": "antfly",
                        "model": "fixed",
                        "text": {"target_tokens": 32},
                    }
                },
                "rerankers": {
                    "relevance": {
                        "provider": "antfly",
                        "model": "mock-rerank",
                        "field": "text",
                    }
                },
                "transcribers": {
                    "speech": {
                        "provider": "antfly",
                        "model": "mock-transcriber",
                        "api_key": "speech-api-key",
                        "bearer_token": "speech-bearer-key",
                        "language_code": "fr",
                        "timestamps": True,
                        "diarization": True,
                    }
                },
                "extractors": {
                    "entities": {
                        "model": "mock-extractor",
                        "connection": "default-inference",
                    }
                },
                "rewriters": {
                    "query": {
                        "model": "mock-rewriter",
                        "connection": "default-inference",
                    }
                },
                "predictors": {
                    "risk": {"model": "mock-risk", "connection": "default-inference"}
                },
                "text_to_speech": {
                    "voice": {
                        "provider": "openai",
                        "model": "mock-voice",
                        "voice": "alloy",
                        "base_url": upstream_url + "/v1",
                        "api_key": "mock-server-key",
                    }
                },
            }
            config["transcribers"]["speech-api"] = {
                "provider": "antfly",
                "model": "mock-transcriber",
                "api_key": "speech-api-key",
            }
            config["transcribers"]["speech-inherit"] = {
                "provider": "antfly",
                "model": "mock-transcriber",
            }
            config["generators"].update(
                {
                    "busy": {"provider": "antfly", "model": "mock-busy"},
                    "external": {
                        "provider": "openai",
                        "model": "mock-chat",
                        "url": upstream_url + "/ai/v1",
                        "api_key": "mock-server-key",
                    },
                    "unauthorized": {
                        "provider": "antfly",
                        "model": "mock-unauthorized",
                    },
                }
            )
            config["embedders"]["sparse"] = {
                "provider": "antfly",
                "model": "mock-sparse",
                "sparse": True,
            }
            config["connections"] = {
                "vendor": {
                    "kind": "inference",
                    "capabilities": ["models.generate"],
                    "inference": {
                        "provider": "openai",
                        "url": upstream_url + "/ai/v1",
                        "api_key": "mock-server-key",
                    },
                }
            }
            config["connections"]["runtime"] = {
                "kind": "inference",
                "capabilities": ["models.generate"],
                "inference": {
                    "provider": "antfly",
                    "url": upstream_url,
                    "api_key": "mock-server-key",
                },
            }
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config))
            port = free_port()
            base_url = f"http://127.0.0.1:{port}"
            with (root / "server.log").open("w+") as log:
                process = subprocess.Popen(
                    [
                        str(args.antfly.resolve()),
                        "standalone",
                        "--config",
                        str(config_path),
                        "--data-dir",
                        str(root / "data"),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--health",
                        "false",
                        "--auth",
                        "false",
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                try:
                    deadline = time.monotonic() + 90
                    while True:
                        if process.poll() is not None:
                            log.seek(0)
                            raise RuntimeError(f"Antfly exited: {log.read()}")
                        try:
                            with urllib.request.urlopen(
                                base_url + "/readyz", timeout=1
                            ) as response:
                                if response.status == 200:
                                    break
                        except (OSError, urllib.error.URLError):
                            pass
                        if time.monotonic() >= deadline:
                            log.seek(0)
                            raise RuntimeError(
                                f"Antfly did not become ready: {log.read()}"
                            )
                        time.sleep(0.1)
                    print("remote-only gateway ready", flush=True)
                    with openai.OpenAI(
                        api_key="client-key",
                        base_url=base_url + "/ai/v1",
                        max_retries=0,
                        timeout=10,
                    ) as client:
                        models = {model.id for model in client.models.list()}
                        assert {
                            "generators/support",
                            "deciders/triage",
                            "embedders/dense",
                            "readers/ocr",
                        } <= models
                        chat = client.chat.completions.create(
                            model="generators/support",
                            messages=[{"role": "user", "content": "hello"}],
                        )
                        assert chat.choices[0].message.content == "hello"
                        assert chat.usage.total_tokens == 3
                        decision = client.decisions.create(
                            model="deciders/triage",
                            input="refund",
                            questions=[
                                {
                                    "name": "refund",
                                    "type": "predicate",
                                    "instructions": "Refund?",
                                }
                            ],
                        )
                        assert decision.answers[0].probability == 0.9
                        # The official Python SDK requests base64 by default and
                        # converts the returned little-endian float32 vectors.
                        embedded = client.embeddings.create(
                            model="embedders/dense", input="hello"
                        )
                        assert list(embedded.data[0].embedding) == [0.25, 0.5]
                        with client.chat.completions.create(
                            model="generators/support",
                            messages=[{"role": "user", "content": "lookup"}],
                            stream=True,
                            tools=[
                                {
                                    "type": "function",
                                    "function": {
                                        "name": "lookup",
                                        "parameters": {"type": "object"},
                                    },
                                }
                            ],
                        ) as stream:
                            arguments = "".join(
                                chunk.choices[0].delta.tool_calls[0].function.arguments
                                or ""
                                for chunk in stream
                            )
                        assert json.loads(arguments) == {"query": "hello"}
                        try:
                            client.chat.completions.create(
                                model="deciders/triage",
                                messages=[{"role": "user", "content": "hello"}],
                            )
                        except openai.BadRequestError:
                            pass
                        else:
                            raise AssertionError("task mismatch was not rejected")
                    sql_request = urllib.request.Request(
                        base_url + "/db/v1/sql",
                        data=json.dumps(
                            {
                                "statement": "SELECT ai_probability('refund', 'Refund?', 'deciders/triage') AS score"
                            }
                        ).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(sql_request, timeout=10) as response:
                        sql = json.load(response)
                    assert sql["rows"][0][0] == 0.9, sql
                    request = urllib.request.Request(
                        base_url + "/ai/v1/read",
                        data=json.dumps(
                            {
                                "model": "readers/ocr",
                                "images": ["data:image/png;base64,aGk="],
                            }
                        ).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(request, timeout=10) as response:
                        read = json.load(response)
                    assert read["data"][0]["text"] == "recognized"
                    assert read["usage"]["total_tokens"] == 3

                    def invoke(operation, value=None, *, method="POST", headers=None):
                        payload = None if value is None else json.dumps(value).encode()
                        request = urllib.request.Request(
                            base_url + operation,
                            data=payload,
                            method=method,
                            headers={
                                "Content-Type": "application/json",
                                **(headers or {}),
                            },
                        )
                        with urllib.request.urlopen(request, timeout=10) as response:
                            raw = response.read()
                            if (
                                "json" in response.headers.get("Content-Type", "")
                                and raw
                            ):
                                return json.loads(raw)
                            return raw

                    catalog = invoke("/ai/v1/models", method="GET")
                    chain = next(
                        item
                        for item in catalog["data"]
                        if item["id"] == "chains/fallback"
                    )
                    assert chain["execution_policy"]["cross_model_fallback"] is True, (
                        chain
                    )
                    assert chain["execution_policy"]["max_attempts"] == 2, chain
                    for alias, expected in (
                        ("speech-api", "speech-api-key"),
                        ("speech-inherit", "mock-server-key"),
                        ("speech", "speech-bearer-key"),
                    ):
                        for operation in ("transcribe", "dictate"):
                            invoke(
                                "/ai/v1/" + operation,
                                {"model": "transcribers/" + alias, "audio": "aGk="},
                            )
                            assert (
                                Upstream.authorizations[-1] == "Bearer " + expected
                            ), Upstream.authorizations[-1]
                        created = invoke(
                            "/ai/v1/transcription/sessions",
                            {"model": "transcribers/" + alias},
                        )
                        assert Upstream.authorizations[-1] == "Bearer " + expected, (
                            Upstream.authorizations[-1]
                        )
                        invoke(
                            "/ai/v1/transcription/sessions/" + created["id"] + "/audio",
                            {"audio": "aGk="},
                        )
                        assert Upstream.authorizations[-1] == "Bearer " + expected, (
                            Upstream.authorizations[-1]
                        )
                        invoke(
                            "/ai/v1/transcription/sessions/" + created["id"],
                            method="DELETE",
                        )
                    prompt = {"messages": [{"role": "user", "content": "hello"}]}
                    for extra in (
                        {"ignore_eos": True},
                        {"max_tokens": 8, "max_completion_tokens": 4},
                    ):
                        before = len(Upstream.chat_attempts)
                        try:
                            invoke(
                                "/ai/v1/generate",
                                {"model": "chains/mixed", **prompt, **extra},
                            )
                            raise AssertionError("invalid chain reached execution")
                        except urllib.error.HTTPError as error:
                            assert error.code == 400, error.read()
                        assert len(Upstream.chat_attempts) == before, (
                            Upstream.chat_attempts[before:]
                        )
                    for path in ("/ai/v1/generate/", "/ai//v1//generate//"):
                        invoke(path, {"model": "mock-chat", **prompt})
                        assert Upstream.requests[-1][0] == "/ai/v1/generate", (
                            Upstream.requests[-1]
                        )
                    for path in ("/ai/v1/generate/", "/ai//v1//generate//"):
                        invoke(
                            path,
                            {
                                "model": "generators/support",
                                **prompt,
                                "ignore_eos": True,
                            },
                        )
                    for path in (
                        "/ai/v1/chat/completions/",
                        "/ai//v1//chat/completions//",
                    ):
                        try:
                            invoke(
                                path,
                                {
                                    "model": "generators/support",
                                    **prompt,
                                    "ignore_eos": True,
                                },
                            )
                            raise AssertionError("chat accepted a native-only option")
                        except urllib.error.HTTPError as error:
                            assert error.code == 400, error.read()
                    for path in ("/ai/v1/embed/", "/ai//v1//embed//"):
                        invoke(path, {"model": "mock-multimodal", "input": ["hello"]})
                        assert Upstream.requests[-1][0] == "/ai/v1/embed", (
                            Upstream.requests[-1]
                        )
                    for path in ("/ai/v1/dictate/", "/ai//v1//dictate//"):
                        result = invoke(
                            path, {"model": "transcribers/speech", "audio": "aGk="}
                        )
                        assert result["object"] == "dictation", result
                        assert Upstream.requests[-1][0] == "/ai/v1/dictate", (
                            Upstream.requests[-1]
                        )
                    before = len(Upstream.chat_attempts)
                    fallback = invoke(
                        "/ai/v1/chat/completions",
                        {"model": "chains/fallback", **prompt},
                    )
                    assert fallback["choices"][0]["message"]["content"] == "hello"
                    assert Upstream.chat_attempts[before:] == ["mock-busy", "mock-chat"]
                    for model, headers, expected_status, expected_attempts in [
                        (
                            "chains/fallback",
                            {"X-Antfly-Inference-Attempts-Remaining": "1"},
                            429,
                            ["mock-busy"],
                        ),
                        ("chains/no-auth-fallback", {}, 401, ["mock-unauthorized"]),
                        (
                            "generators/support",
                            {"X-Antfly-Inference-Attempts-Remaining": "0"},
                            429,
                            [],
                        ),
                        (
                            "chains/fallback",
                            {"X-Antfly-Inference-Timeout-Ms": "0"},
                            504,
                            [],
                        ),
                    ]:
                        before = len(Upstream.chat_attempts)
                        try:
                            invoke(
                                "/ai/v1/chat/completions",
                                {"model": model, **prompt},
                                headers=headers,
                            )
                            raise AssertionError(
                                "failure policy did not reject request"
                            )
                        except urllib.error.HTTPError as error:
                            assert error.code == expected_status, error.read()
                        assert Upstream.chat_attempts[before:] == expected_attempts
                    assert (
                        abs(
                            invoke(
                                "/ai/v1/rerank",
                                {
                                    "model": "rerankers/relevance",
                                    "query": "hello",
                                    "documents": ["hello"],
                                },
                            )["data"][0]["score"]
                            - 0.9
                        )
                        < 1e-6
                    )
                    assert (
                        invoke(
                            "/ai/v1/chunk",
                            {"model": "chunkers/paragraphs", "input": "hello"},
                        )["data"][0]["end_char"]
                        == 5
                    )
                    assert invoke(
                        "/ai/v1/rewrite",
                        {"model": "rewriters/query", "inputs": ["hello"]},
                    )["data"][0]["texts"] == ["rewritten"]
                    assert (
                        invoke(
                            "/ai/v1/extract",
                            {
                                "model": "extractors/entities",
                                "schema_version": 2,
                                "input": ["Ada"],
                            },
                        )["data"][0]["entities"][0]["end"]
                        == 3
                    )
                    assert (
                        invoke(
                            "/ai/v1/transcribe",
                            {"model": "transcribers/speech", "audio": "aGk="},
                        )["data"][0]["segments"][0]["start_ms"]
                        == 10
                    )
                    assert (
                        invoke(
                            "/ai/v1/dictate",
                            {"model": "transcribers/speech", "audio": "aGk="},
                        )["text"]
                        == "hello"
                    )
                    assert (
                        invoke(
                            "/ai/v1/synthesize",
                            {"model": "synthesizers/voice", "text": "hello"},
                        )
                        == b"ID3mock-audio"
                    )
                    assert invoke(
                        "/ml/v1/predict",
                        {"model": "predictors/risk", "input": [[1, 2]]},
                    )["predictions"] == [[0.5]]
                    schema = invoke("/ml/v1/models", method="GET")["predictors"][
                        "predictors/risk"
                    ]
                    assert schema["feature_names"] == ["first", "second"]
                    assert invoke(
                        "/ai/v1/embed", {"model": "embedders/sparse", "input": "hello"}
                    )["data"][0]["embedding"]["indices"] == [1, 9]
                    batch = invoke(
                        "/ai/v1/generate/batch",
                        {
                            "requests": [
                                {
                                    "custom_id": "one",
                                    "body": {"model": "generators/support", **prompt},
                                },
                                {
                                    "custom_id": "two",
                                    "body": {"model": "generators/support", **prompt},
                                },
                            ]
                        },
                    )
                    assert batch["summary"]["succeeded"] == 2
                    # Socket-free batch execution must retain both the native
                    # transport contract and the parent's per-item attempt cap.
                    for model in ("mock-chat", "connections/runtime/models/mock-chat"):
                        raw_batch = invoke(
                            "/ai/v1/generate/batch",
                            {
                                "requests": [
                                    {
                                        "custom_id": "raw",
                                        "body": {"model": model, **prompt},
                                    }
                                ]
                            },
                        )
                        assert raw_batch["summary"]["succeeded"] == 1, raw_batch
                        assert (
                            raw_batch["data"][0]["response"]["choices"][0]["message"][
                                "content"
                            ]
                            == "hello"
                        )
                    before_batch = len(Upstream.chat_attempts)
                    limited = invoke(
                        "/ai/v1/generate/batch",
                        {
                            "requests": [
                                {
                                    "custom_id": "limited",
                                    "body": {"model": "chains/fallback", **prompt},
                                }
                            ]
                        },
                        headers={"X-Antfly-Inference-Attempts-Remaining": "1"},
                    )
                    assert limited["summary"]["failed"] == 1, limited
                    assert Upstream.chat_attempts[before_batch:] == ["mock-busy"], (
                        Upstream.chat_attempts[before_batch:]
                    )
                    for operation in ("transcribe", "dictate"):
                        invoke(
                            "/ai/v1/" + operation,
                            {"model": "transcribers/speech", "audio": "aGk="},
                        )
                        assert Upstream.requests[-1][1] == {
                            "model": "mock-transcriber",
                            "audio": "aGk=",
                            "language": "fr",
                            "timestamps": True,
                            "diarization": True,
                        }, Upstream.requests[-1]
                        invoke(
                            "/ai/v1/" + operation,
                            {
                                "model": "transcribers/speech",
                                "audio": "aGk=",
                                "language": None,
                                "timestamps": None,
                                "diarization": None,
                            },
                        )
                        assert Upstream.requests[-1][1] == {
                            "model": "mock-transcriber",
                            "audio": "aGk=",
                            "language": "fr",
                            "timestamps": True,
                            "diarization": True,
                        }, Upstream.requests[-1]
                        invoke(
                            "/ai/v1/" + operation,
                            {
                                "model": "transcribers/speech",
                                "audio": "aGk=",
                                "language": "de",
                                "timestamps": False,
                                "diarization": False,
                            },
                        )
                        assert Upstream.requests[-1][1] == {
                            "model": "mock-transcriber",
                            "audio": "aGk=",
                            "language": "de",
                            "timestamps": False,
                            "diarization": False,
                        }, Upstream.requests[-1]
                    session = invoke(
                        "/ai/v1/transcription/sessions",
                        {"model": "transcribers/speech"},
                    )
                    assert Upstream.requests[-1][1] == {
                        "model": "mock-transcriber",
                        "language": "fr",
                    }, Upstream.requests[-1]
                    for supplied, expected in ((None, "fr"), ("de", "de")):
                        extra_session = invoke(
                            "/ai/v1/transcription/sessions",
                            {
                                "model": "transcribers/speech",
                                "language": supplied,
                                "emit_partials": False,
                            },
                        )
                        assert Upstream.requests[-1][1] == {
                            "model": "mock-transcriber",
                            "language": expected,
                            "emit_partials": False,
                        }, Upstream.requests[-1]
                        invoke(
                            "/ai/v1/transcription/sessions/" + extra_session["id"],
                            method="DELETE",
                        )
                    session_id = session["id"]
                    assert session_id != "backend-session"
                    session_path = "/ai/v1/transcription/sessions/" + session_id
                    assert invoke(session_path, method="GET")["id"] == session_id
                    assert (
                        invoke(session_path + "/audio", {"audio": "aGk="})["session_id"]
                        == session_id
                    )
                    events = invoke(session_path + "/events", method="GET").decode()
                    assert (
                        session_id in events
                        and "backend-session" not in events
                        and "[DONE]" in events
                    )
                    invoke(session_path, method="DELETE")
                    qualified = invoke(
                        "/ai/v1/chat/completions",
                        {"model": "connections/vendor/models/mock-chat", **prompt},
                    )
                    assert qualified["choices"][0]["message"]["content"] == "hello"
                    virtual = invoke(
                        "/db/v1/connections/named-inference/inference/generate",
                        {"model": "generators/support", **prompt},
                    )
                    assert virtual["choices"][0]["message"]["content"] == "hello"
                    try:
                        invoke(
                            "/ai/v1/embeddings",
                            {
                                "model": "connections/vendor/models/mock-chat",
                                "input": "hello",
                            },
                        )
                        raise AssertionError(
                            "connection capability denial was bypassed"
                        )
                    except urllib.error.HTTPError as error:
                        assert error.code == 403, error.read()
                    assert not any(
                        path.is_file() for path in (root / "no-model-files").rglob("*")
                    )
                    print(
                        json.dumps(
                            {
                                "openai_version": openai.__version__,
                                "upstream_calls": Upstream.calls,
                                "result": "passed",
                            }
                        ),
                        flush=True,
                    )
                except BaseException:
                    print((root / "server.log").read_text(), flush=True)
                    raise
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
    finally:
        upstream.shutdown()
        upstream.server_close()


if __name__ == "__main__":
    main()
