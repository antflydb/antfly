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
import base64
import random
import http.client
import io
import struct
import json
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zlib
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import openai


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    model_delay_seconds: ClassVar[float] = 0
    session_creation_delay: ClassVar[float] = 0
    session_idle_ttl: ClassVar[int] = 0
    session_delete_status: ClassVar[int] = 204
    session_gets: ClassVar[int] = 0
    session_append_delay: ClassVar[float] = 0
    session_reply_override: ClassVar[bool] = False
    session_reply: ClassVar[object] = None
    image_stream_release: ClassVar[threading.Event | None] = None
    calls: ClassVar[list[str]] = []
    chat_attempts: ClassVar[list[str]] = []
    requests: ClassVar[list[tuple[str, dict]]] = []
    authorizations: ClassVar[list[str | None]] = []

    def log_message(self, *_args):
        pass

    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            # Deliberate stream failures/cancellation close pooled connections.
            pass

    def reply(self, value, status=200, content_type="application/json"):
        encoded = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def image_stream(self, packets, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            for index, packet in enumerate(packets):
                # Deliberately split native frames and SSE records across writes.
                for offset in range(0, len(packet), 13):
                    self.wfile.write(packet[offset : offset + 13])
                self.wfile.flush()
                if index == 0 and self.image_stream_release is not None:
                    assert self.image_stream_release.wait(10), (
                        "gateway buffered the live upstream stream"
                    )
        except (BrokenPipeError, ConnectionResetError):
            pass

    def anthropic_chat(self, request):
        assert self.headers.get("x-api-key") == "mock-server-key"
        assert self.headers.get("anthropic-version") == "2026-10-21"
        if request["model"] != "new-vision-model":
            assert self.headers.get("anthropic-workspace-id") == "fixture-workspace"
        self.calls.append(self.path)
        model = request["model"]
        streaming = request.get("stream", False)
        if model in ("anthropic-validation", "anthropic-busy"):
            kind = (
                "invalid_request_error"
                if model.endswith("validation")
                else "overloaded_error"
            )
            if streaming:
                self.image_stream(
                    [
                        anthropic_event(
                            {
                                "type": "error",
                                "error": {"type": kind, "message": "fixture"},
                            }
                        )
                    ],
                    "text/event-stream",
                )
            else:
                self.reply(
                    {"type": "error", "error": {"type": kind}},
                    status=400 if model.endswith("validation") else 529,
                )
            return
        if model == "long":
            self.anthropic_long_stream()
            return
        history = request["messages"]
        replay = any(
            part["type"] == "tool_result"
            for message in history
            for part in message["content"]
        )
        if replay:
            previous = next(
                message
                for message in reversed(history)
                if message["role"] == "assistant"
            )
            assert previous["content"][0] == {
                "type": "thinking",
                "thinking": "private",
                "signature": "native-signature",
            }, previous
        for message in history:
            assert not any(
                part["type"] == "text" and not part["text"].strip()
                for part in message["content"]
            ), message
        for tool in request.get("tools", []):
            assert tool["input_schema"]["type"] == "object", tool
            if tool["input_schema"].get("properties") == {}:
                assert tool["input_schema"]["additionalProperties"] is False
            if tool["name"] == "strict_lookup":
                assert tool["strict"] is True, tool
        tools = bool(request.get("tools")) and not replay
        if "output_config" in request:
            assert request["output_config"]["format"]["type"] == "json_schema"
            assert request["output_config"]["format"]["schema"]["type"] == "object"
        content = [
            {"type": "thinking", "thinking": "private", "signature": "native-signature"}
        ]
        content += (
            [
                {
                    "type": "tool_use",
                    "id": "anthropic-call",
                    "name": "lookup",
                    "input": {"id": 7},
                }
            ]
            if tools
            else [
                {
                    "type": "text",
                    "text": '{"compared":true}'
                    if "output_config" in request
                    else "anthropic hello",
                }
            ]
        )
        reason = (
            "max_tokens"
            if model == "anthropic-truncated"
            else "refusal"
            if model == "anthropic-filtered"
            else "tool_use"
            if tools
            else "end_turn"
        )
        usage = {"input_tokens": 2, "cache_read_input_tokens": 2, "output_tokens": 1}
        message = {
            "id": "msg-anthropic",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": content,
            "stop_reason": reason,
            "usage": usage,
        }
        if not streaming:
            self.reply(message)
            return
        start = {**message, "content": [], "stop_reason": None}
        events = [{"type": "message_start", "message": start}]
        if model == "anthropic-midstream":
            events += [
                {
                    "type": "error",
                    "error": {"type": "overloaded_error", "message": "failed"},
                }
            ]
        else:
            for index, block in enumerate(content):
                initial = {**block}
                if block["type"] == "thinking":
                    initial = {"type": "thinking", "thinking": ""}
                    deltas = [
                        {"type": "thinking_delta", "thinking": block["thinking"]},
                        {"type": "signature_delta", "signature": block["signature"]},
                    ]
                elif block["type"] == "tool_use":
                    initial["input"] = {}
                    deltas = [
                        {"type": "input_json_delta", "partial_json": '{"id":'},
                        {"type": "input_json_delta", "partial_json": "7}"},
                    ]
                else:
                    initial["text"] = ""
                    deltas = [{"type": "text_delta", "text": block["text"]}]
                events.append(
                    {
                        "type": "content_block_start",
                        "index": index,
                        "content_block": initial,
                    }
                )
                events.extend(
                    {"type": "content_block_delta", "index": index, "delta": delta}
                    for delta in deltas
                )
                events.append({"type": "content_block_stop", "index": index})
            events += [
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": reason},
                    "usage": {"output_tokens": 1},
                },
                {"type": "message_stop"},
            ]
        self.image_stream(
            [anthropic_event(event) for event in events], "text/event-stream"
        )

    def anthropic_long_stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            for event in (
                {
                    "type": "message_start",
                    "message": {
                        "role": "assistant",
                        "usage": {"input_tokens": 2, "output_tokens": 0},
                    },
                },
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                },
            ):
                self.wfile.write(anthropic_event(event))
            packet = anthropic_event(
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "x"},
                }
            )
            assert len(packet) * 40000 > 4 * 1024 * 1024
            for index in range(40000):
                self.wfile.write(packet)
                if index % 128 == 0:
                    self.wfile.flush()
            for event in (
                {"type": "content_block_stop", "index": 0},
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "max_tokens"},
                    "usage": {"output_tokens": 40000},
                },
                {"type": "message_stop"},
            ):
                self.wfile.write(anthropic_event(event))
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def long_chat_stream(self, bedrock=False):
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/vnd.amazon.eventstream" if bedrock else "text/event-stream",
        )
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

        def chunk(delta, finish=None):
            return (
                "data: "
                + json.dumps(
                    {
                        "id": "long-response",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": "long",
                        "choices": [
                            {"index": 0, "delta": delta, "finish_reason": finish}
                        ],
                    }
                )
                + "\n\n"
            ).encode()

        try:
            self.wfile.write(
                bedrock_frame("messageStart", {"role": "assistant"})
                if bedrock
                else chunk({"role": "assistant"})
            )
            packet = (
                bedrock_frame(
                    "contentBlockDelta",
                    {"contentBlockIndex": 0, "delta": {"text": "x"}},
                )
                if bedrock
                else chunk({"content": "x"})
            )
            assert len(packet) * 40000 > 4 * 1024 * 1024
            for index in range(40000):
                self.wfile.write(packet)
                if index % 128 == 0:
                    self.wfile.flush()
            if bedrock:
                for tag, value in (
                    ("contentBlockStop", {"contentBlockIndex": 0}),
                    ("messageStop", {"stopReason": "max_tokens"}),
                    (
                        "metadata",
                        {
                            "usage": {
                                "inputTokens": 2,
                                "outputTokens": 40000,
                                "totalTokens": 40002,
                            }
                        },
                    ),
                ):
                    self.wfile.write(bedrock_frame(tag, value))
            else:
                self.wfile.write(chunk({}, "length") + b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def google_chat(self, request):
        if request["model"] == "google/deadline-model":
            time.sleep(0.65)
        assert self.headers.get("Authorization") == "Bearer mock-server-key"
        if "mock-vision-busy" in request["model"]:
            self.reply({"error": "busy"}, status=503)
            return
        self.calls.append(self.path)
        if request["model"].endswith("long"):
            self.long_chat_stream()
            return
        history = request["messages"]
        replay = any(message["role"] == "tool" for message in history)
        if replay:
            previous = next(
                message for message in reversed(history) if message.get("tool_calls")
            )
            assert (
                previous["tool_calls"][0]["extra_content"]["google"][
                    "thought_signature"
                ]
                == "opaque-signature"
            )
        structured = "response_format" in request
        tools = bool(request.get("tools")) and not replay
        tool = {
            "id": "google-call-1",
            "type": "function",
            "function": {"name": "lookup", "arguments": '{"id":7}'},
            "extra_content": {"google": {"thought_signature": "opaque-signature"}},
        }
        usage = (
            None
            if "mock-unknown" in request["model"]
            else {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 5}
        )
        if not request.get("stream"):
            self.reply(
                {
                    "id": "google-response",
                    "object": "chat.completion",
                    "created": 1,
                    "model": request["model"],
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": None
                                if tools
                                else (
                                    '{"compared":true}'
                                    if structured
                                    else "google hello"
                                ),
                                **({"tool_calls": [tool]} if tools else {}),
                            },
                            "finish_reason": "tool_calls" if tools else "stop",
                        }
                    ],
                    "usage": usage,
                }
            )
            return

        def chunk(delta, reason=None):
            return (
                "data: "
                + json.dumps(
                    {
                        "id": "google-response",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": request["model"],
                        "choices": [
                            {"index": 0, "delta": delta, "finish_reason": reason}
                        ],
                    }
                )
                + "\n\n"
            ).encode()

        packets = [chunk({"role": "assistant"})]
        if tools:
            packets += [
                chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                **tool,
                                "function": {"name": "lookup", "arguments": ""},
                            }
                        ]
                    }
                ),
                chunk(
                    {"tool_calls": [{"index": 0, "function": {"arguments": '{"id":'}}]}
                ),
                chunk({"tool_calls": [{"index": 0, "function": {"arguments": "7}"}}]}),
            ]
        else:
            packets += [chunk({"content": "google "}), chunk({"content": "hello"})]
        packets += [chunk({}, "tool_calls" if tools else "stop")]
        if request.get("stream_options", {}).get("include_usage"):
            packets += [
                (
                    "data: "
                    + json.dumps(
                        {
                            "id": "google-response",
                            "object": "chat.completion.chunk",
                            "created": 1,
                            "model": request["model"],
                            "choices": [],
                            "usage": usage,
                        }
                    )
                    + "\n\n"
                ).encode()
            ]
        packets += [b"data: [DONE]\n\n"]
        self.image_stream(packets, "text/event-stream")

    def bedrock_chat(self, request):
        assert self.headers.get("Authorization") == "Bearer mock-server-key"
        self.calls.append(self.path)
        streaming = self.path.endswith("/converse-stream")
        assert not any(
            "text" in part and not part["text"].strip()
            for message in request["messages"]
            for part in message["content"]
        ), request
        if "/validation/" in self.path:
            if streaming:
                self.image_stream(
                    [
                        bedrock_frame(
                            "validationException",
                            {"message": "Invalid input"},
                            "exception",
                        )
                    ],
                    "application/vnd.amazon.eventstream",
                )
            else:
                self.reply({"message": "Invalid input"}, status=400)
            return
        if "/long/" in self.path:
            self.long_chat_stream(bedrock=True)
            return
        structured = "outputConfig" in request
        if structured:
            fmt = request["outputConfig"]["textFormat"]
            assert fmt["type"] == "json_schema", fmt
            assert (
                json.loads(fmt["structure"]["jsonSchema"]["schema"])["type"] == "object"
            ), fmt
        tools = bool(request.get("toolConfig")) and not any(
            "toolResult" in part
            for message in request["messages"]
            for part in message["content"]
        )
        if "stream-busy" in self.path:
            if streaming:
                self.image_stream(
                    [
                        bedrock_frame(
                            "throttlingException", {"message": "busy"}, "exception"
                        )
                    ],
                    "application/vnd.amazon.eventstream",
                )
            else:
                self.reply({"error": "busy"}, status=429)
            return
        reasoning = [
            {
                "reasoningContent": {
                    "reasoningText": {
                        "text": "private",
                        "signature": "native-signature",
                    }
                }
            },
            {"reasoningContent": {"redactedContent": "cmVkYWN0ZWQ="}},
        ]
        unsigned = "/unsigned/" in self.path
        if unsigned:
            del reasoning[0]["reasoningContent"]["reasoningText"]["signature"]
        if any(
            "toolResult" in block
            for turn in request["messages"]
            for block in turn["content"]
        ):
            previous = next(
                turn
                for turn in reversed(request["messages"])
                if turn["role"] == "assistant"
            )
            assert previous["content"][:2] == reasoning, previous
        for tool in request.get("toolConfig", {}).get("tools", []):
            spec = tool["toolSpec"]
            assert spec["inputSchema"]["json"]["type"] == "object", spec
            if spec["inputSchema"]["json"].get("properties") == {}:
                assert spec["inputSchema"]["json"]["additionalProperties"] is False
            if spec["name"] == "strict_lookup":
                assert spec["strict"] is True, spec
        usage = {"inputTokens": 2, "outputTokens": 1, "totalTokens": 5}
        if not streaming:
            content = (
                [
                    {
                        "toolUse": {
                            "toolUseId": "bedrock-call-1",
                            "name": "lookup",
                            "input": {"id": 7},
                        }
                    }
                ]
                if tools
                else [{"text": '{"compared":true}' if structured else "bedrock hello"}]
            )
            self.reply(
                {
                    "output": {
                        "message": {"role": "assistant", "content": reasoning + content}
                    },
                    "stopReason": "max_tokens"
                    if "/truncated/" in self.path
                    else "content_filtered"
                    if "/filtered/" in self.path
                    else "tool_use"
                    if tools
                    else "end_turn",
                    "usage": usage,
                }
            )
            return
        packets = [bedrock_frame("messageStart", {"role": "assistant"})]
        if "midstream" in self.path:
            packets += [
                bedrock_frame(
                    "throttlingException", {"message": "interrupted"}, "exception"
                )
            ]
        else:
            deltas = [{"text": "private"}]
            if not unsigned:
                deltas.append({"signature": "native-signature"})
            for delta in deltas:
                packets.append(
                    bedrock_frame(
                        "contentBlockDelta",
                        {"contentBlockIndex": 0, "delta": {"reasoningContent": delta}},
                    )
                )
            packets.append(bedrock_frame("contentBlockStop", {"contentBlockIndex": 0}))
            packets.append(
                bedrock_frame(
                    "contentBlockDelta",
                    {
                        "contentBlockIndex": 1,
                        "delta": {
                            "reasoningContent": {"redactedContent": "cmVkYWN0ZWQ="}
                        },
                    },
                )
            )
            packets.append(bedrock_frame("contentBlockStop", {"contentBlockIndex": 1}))
            if tools:
                packets += [
                    bedrock_frame(
                        "contentBlockStart",
                        {
                            "contentBlockIndex": 2,
                            "start": {
                                "toolUse": {
                                    "toolUseId": "bedrock-call-1",
                                    "name": "lookup",
                                }
                            },
                        },
                    ),
                    bedrock_frame(
                        "contentBlockDelta",
                        {
                            "contentBlockIndex": 2,
                            "delta": {"toolUse": {"input": '{"id":'}},
                        },
                    ),
                    bedrock_frame(
                        "contentBlockDelta",
                        {"contentBlockIndex": 2, "delta": {"toolUse": {"input": "7}"}}},
                    ),
                ]
            else:
                packets += [
                    bedrock_frame(
                        "contentBlockDelta",
                        {"contentBlockIndex": 2, "delta": {"text": "bedrock "}},
                    ),
                    bedrock_frame(
                        "contentBlockDelta",
                        {"contentBlockIndex": 2, "delta": {"text": "hello"}},
                    ),
                ]
            packets += [
                bedrock_frame("contentBlockStop", {"contentBlockIndex": 2}),
                bedrock_frame(
                    "messageStop", {"stopReason": "tool_use" if tools else "end_turn"}
                ),
                bedrock_frame("metadata", {"usage": usage}),
            ]
        self.image_stream(packets, "application/vnd.amazon.eventstream")

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/anthropic/models":
            assert self.headers.get("x-api-key") == "mock-server-key"
            assert self.headers.get("anthropic-version") == "2026-10-21"
            assert self.headers.get("anthropic-workspace-id") == "fixture-workspace"
            self.reply(
                {
                    "data": [
                        {
                            "id": "future-claude",
                            "display_name": "Future Claude",
                            "created_at": "2026-10-23T00:00:00Z",
                        }
                    ]
                }
            )
            return
        if path == "/ai/v1/models":
            time.sleep(self.model_delay_seconds)
            self.reply(
                {
                    "object": "list",
                    "data": [
                        {"id": "mock-chat", "object": "model"},
                        {"id": "mock-data-only", "tasks": ["generate", "embed"]},
                    ],
                    "generators": {"mock-chat": {}, "mock-shared": {}},
                    "deciders": {"mock-decision": {}},
                    "classifiers": {"mock-shared": {}, "mock-decision": {}},
                    "embedders": {
                        "mock-sparse": {},
                        "mock-multimodal": {},
                        "mock-shared": {},
                        "mock-overflow": {"dimensions": 4294967296},
                        "mock-huge": {"dimensions": 9223372036854775807},
                    },
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
                b': heartbeat\r\nid: cursor\r\nevent: session.open\r\ndata: {"type":"session.open",\r\ndata: "session_id":"backend-session"}\r\n\r\ndata: [DONE]\r\n\r\n',
                content_type="text/event-stream",
            )
        elif path == "/ai/v1/transcription/sessions/backend-session":
            type(self).session_gets += 1
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
        self.reply(b"", self.session_delete_status)

    def extra(self, request):
        path = self.path
        if path == "/ai/v1/generate" and request.get("messages", [{}])[0].get(
            "content"
        ) in (
            "malformed-batch-response",
            "error-batch-response",
            "incomplete-batch-response",
        ):
            self.reply(
                {
                    "malformed-batch-response": b"invalid-json",
                    "error-batch-response": {"error": {"message": "backend failed"}},
                    "incomplete-batch-response": {"model": "backend", "choices": []},
                }[request["messages"][0]["content"]]
            )
            return True
        if path == "/ai/v1/transcription/sessions" and self.session_idle_ttl:
            created = int(time.time())
            self.reply(
                {
                    "id": "backend-session",
                    "created": created,
                    "expires_at": created + self.session_idle_ttl,
                }
            )
            return True
        if (
            path == "/ai/v1/transcription/sessions/backend-session/audio"
            and self.session_append_delay
        ):
            time.sleep(self.session_append_delay)
            self.reply({"session_id": "backend-session", "events": []})
            return True
        if path in ("/ai/v1/generate", "/ai/v1/chat/completions"):
            self.chat_attempts.append(request["model"])
            if request["model"] in ("mock-busy", "mock-unauthorized"):
                self.calls.append(path)
                self.reply(
                    {"error": {"message": "controlled failure"}},
                    503 if request["model"] == "mock-busy" else 401,
                )
                return True
        if path == "/ai/v1/generate":
            assert request["model"] == "mock-chat"
            self.calls.append(path)
            if request.get("stream"):
                self.reply(
                    b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n',
                    content_type="text/event-stream",
                )
                return True
            self.reply(
                {
                    "id": "mock-generate",
                    "object": "chat.completion",
                    "created": 1,
                    "model": request["model"],
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "hello",
                                "refusal": None,
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 2,
                        "completion_tokens": 1,
                        "total_tokens": 3,
                        "provider_tokens": 7,
                    },
                    "provider_extension": {"preserved": True},
                }
            )
            return True
        if path == "/ai/v1/chat/completions":
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
            if path == "/ai/v1/transcription/sessions" and self.session_creation_delay:
                time.sleep(self.session_creation_delay)
                self.reply(
                    {"id": "backend-session", "expires_at": int(time.time()) + 2}
                )
                return True
            self.calls.append(path)
            if path == "/ai/v1/chunk":
                assert request["config"]["model"] == "fixed"
            self.reply(
                self.session_reply
                if path == "/ai/v1/transcription/sessions"
                and self.session_reply_override
                else responses[path]
            )
            return True
        return False

    def do_POST(self):
        if self.path == "/mock-token":
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            time.sleep(0.65)
            self.reply(
                {
                    "access_token": "mock-server-key",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                }
            )
            return
        if self.path.startswith(
            "/ai/v1/transcription/sessions/backend-session/stream?"
        ):
            assert self.path.endswith(
                "format=pcm_f32&sample_rate=48000&commit=false"
            ), self.path
            assert self.headers.get("Content-Type") == "application/octet-stream"
            assert self.headers.get("Transfer-Encoding") == "chunked"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(
                b'event: session.open\r\ndata: {"type":"session.open",\r\ndata: "session_id":"backend-session"}\r\n\r\n'
            )
            self.wfile.flush()
            received = bytearray()
            while True:
                count = int(self.rfile.readline().strip(), 16)
                if count == 0:
                    assert self.rfile.readline() == b"\r\n"
                    break
                received.extend(self.rfile.read(count))
                assert self.rfile.read(2) == b"\r\n"
                self.wfile.write(
                    b'data: {"type":"partial","session_id":"backend-session"}\n\n'
                )
                self.wfile.flush()
            assert received == b"pcm!more", received
            if self.session_append_delay:
                time.sleep(self.session_append_delay)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        if self.headers.get("Content-Type") == "application/vnd.antfly.attachments.v1":
            assert self.path == "/ai/v1/transcription/sessions/backend-session/audio"
            body = self.rfile.read(int(self.headers["Content-Length"]))
            assert body.startswith(b"AFATT001") and body.endswith(b"pcm!")
            self.reply({"session_id": "backend-session", "events": []})
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append((self.path, request))
        self.authorizations.append(self.headers.get("Authorization"))
        if self.path.endswith("/anthropic/messages"):
            self.anthropic_chat(request)
            return
        if self.path.endswith(("/converse", "/converse-stream")):
            self.bedrock_chat(request)
            return
        if self.path.endswith(
            ("/openai/chat/completions", "/openapi/chat/completions")
        ):
            self.google_chat(request)
            return
        if self.path.endswith(":generateContent"):
            if "mock-vision-busy" in self.path:
                self.reply({"error": "busy"}, status=503)
                return
            assert (
                self.headers.get("x-goog-api-key") == "mock-server-key"
                or self.headers.get("Authorization") == "Bearer mock-server-key"
            )
            self.calls.append(self.path)
            response = {
                "candidates": [{"content": {"parts": [{"text": "google hello"}]}}]
            }
            if "mock-known" in self.path:
                response["usageMetadata"] = {
                    "promptTokenCount": 2,
                    "candidatesTokenCount": 1,
                    "totalTokenCount": 5,
                }
            self.reply(response)
            return
        assert self.headers.get("Authorization") in (
            "Bearer mock-server-key",
            "Bearer speech-api-key",
            "Bearer speech-bearer-key",
        )
        if self.extra(request):
            return
        if request["model"].startswith("openai-audio-"):
            self.google_chat(request)
            return
        expected = {
            "/ai/v1/chat/completions": "mock-chat",
            "/ai/v1/decisions": "mock-decision",
            "/v1/embeddings": "mock-embedding",
            "/ai/v1/read": "mock-reader",
        }
        assert request["model"] == expected[self.path] or (
            self.path == "/ai/v1/chat/completions"
            and request["model"].startswith("openai-quota-")
        ), (self.path, request)
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
                self.wfile.write(
                    b'data: {"id":"mock-stream","object":"chat.completion.chunk","created":1,"model":"mock-chat","choices":[],\r\ndata: "usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":5}}\r\n\r\n'
                )
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
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def verify_nested_gateway(binary: Path, root: Path, inner_url: str) -> None:
    """Native operations must survive named remote aliases and chains."""
    config = {
        "public_inference": {"enabled": True},
        "inference": {
            "api_url": inner_url,
            "api_key": "mock-server-key",
            "models_dir": str(root / "no-model-files"),
        },
        "generators": {
            "support": {"provider": "antfly", "model": "generators/support"}
        },
        "connections": {
            "remote": {
                "kind": "inference",
                "capabilities": [
                    "models.generate",
                    "models.embed",
                    "models.synthesize",
                    "models.predict",
                ],
                "inference": {
                    "provider": "antfly",
                    "url": inner_url,
                    "api_key": "mock-server-key",
                },
            }
        },
        "chains": {"fallback": [{"generator": "support"}]},
        "inference_capabilities": {"generators/support": {"streaming": "supported"}},
    }
    config["connections"]["prediction-denied"] = {
        "kind": "inference",
        "capabilities": ["models.generate"],
        "inference": {"provider": "antfly", "url": inner_url},
    }
    config_path = root / "nested-config.json"
    config_path.write_text(json.dumps(config))
    base = f"http://127.0.0.1:{free_port()}"
    with (root / "nested-server.log").open("w+") as log:
        process = subprocess.Popen(
            [
                str(binary.resolve()),
                "standalone",
                "--config",
                str(config_path),
                "--data-dir",
                str(root / "nested-data"),
                "--host",
                "127.0.0.1",
                "--port",
                base.rsplit(":", 1)[1],
                "--health",
                "false",
                "--auth",
                "false",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError("nested gateway exited")
                try:
                    with urllib.request.urlopen(
                        base + "/readyz", timeout=1
                    ) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError("nested gateway readiness timed out")
                time.sleep(0.1)
            with urllib.request.urlopen(base + "/ai/v1/models", timeout=10) as response:
                catalog = json.load(response)
            discovered = {(entry["id"], entry.get("task")) for entry in catalog["data"]}
            for model, task in (
                ("chains/fallback", "generate"),
                ("synthesizers/voice", "synthesize"),
            ):
                assert (
                    f"connections/remote/models/{model}",
                    task,
                ) in discovered, catalog
            assert len(discovered) == len(catalog["data"]), catalog
            predictor_id = "connections/remote/models/predictors/risk"
            schema = catalog["predictors"][predictor_id]
            assert schema["num_features"] == 2 and schema["feature_names"] == [
                "first",
                "second",
            ], schema
            assert (predictor_id, "predict") in discovered, catalog
            assert not any(
                key.startswith("connections/prediction-denied/")
                for key in catalog["predictors"]
            ), catalog
            for path in ("/ml/v1/models", "/ml/v1/predictors"):
                with urllib.request.urlopen(base + path, timeout=10) as response:
                    assert json.load(response)["predictors"][predictor_id] == schema
            prediction = urllib.request.Request(
                base + "/ml/v1/predict",
                data=json.dumps({"model": predictor_id, "input": [[1, 2]]}).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(prediction, timeout=10) as response:
                assert response.status == 200

            nested_multi = next(
                entry
                for entry in catalog["data"]
                if entry["id"]
                == "connections/remote/models/connections/runtime/models/mock-data-only"
            )
            assert set(nested_multi["tasks"]) == {"generate", "embed"}, nested_multi
            with urllib.request.urlopen(
                base + "/db/v1/connections?include=models", timeout=10
            ) as response:
                connections = json.load(response)["connections"]
            remote = next(entry for entry in connections if entry["id"] == "remote")
            models = remote["inference"]["models"]
            for group, name in (
                ("chains", "chains/fallback"),
                ("synthesizers", "synthesizers/voice"),
            ):
                assert any(entry["name"] == name for entry in models[group]), remote
            for model in ("generators/support", "chains/fallback"):
                for stream in (False, True):
                    before = len(Upstream.requests)
                    request = urllib.request.Request(
                        base + "/ai/v1/generate",
                        data=json.dumps(
                            {
                                "model": model,
                                "messages": [{"role": "user", "content": "hello"}],
                                "ignore_eos": True,
                                "stream": stream,
                            }
                        ).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(request, timeout=10) as response:
                        body = response.read()
                        if stream:
                            assert response.headers["Content-Type"].startswith(
                                "text/event-stream"
                            )
                            assert b"[DONE]" in body
                        else:
                            assert (
                                json.loads(body)["choices"][0]["message"]["content"]
                                == "hello"
                            )
                    assert len(Upstream.requests) == before + 1
                    path, wire = Upstream.requests[-1]
                    assert path == "/ai/v1/generate", (path, wire)
                    assert wire["ignore_eos"] is True
        except BaseException:
            log.seek(0)
            print(log.read(), flush=True)
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def verify_session_transports(base_url: str, session_path: str) -> None:
    metadata = b'{"audio":"attachment:0","format":"pcm16","sample_rate":16000}'
    mime = b"audio/pcm"
    frame = (
        b"AFATT001"
        + struct.pack("<QII", len(metadata), 1, 0)
        + struct.pack("<IIQ", len(mime), 0, 4)
        + metadata
        + mime
        + b"pcm!"
    )
    with urllib.request.urlopen(
        urllib.request.Request(
            base_url + session_path + "/audio",
            data=frame,
            headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
        ),
        timeout=5,
    ) as response:
        assert json.load(response)["session_id"] == session_path.rsplit("/", 1)[1]
    authority = base_url.removeprefix("http://")
    for known_length in (False, True):
        connection = http.client.HTTPConnection(authority, timeout=5)
        try:
            connection.putrequest(
                "POST",
                session_path + "/stream?format=pcm_f32&sample_rate=48000&commit=false",
            )
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader(
                "Content-Length" if known_length else "Transfer-Encoding",
                "8" if known_length else "chunked",
            )
            connection.endheaders()
            response = connection.getresponse()
            assert response.status == 200

            # This event must arrive while the upload is still empty/open.
            def read_event():
                event = bytearray()
                while True:
                    line = response.readline()
                    assert line, "stream ended before the event delimiter"
                    event.extend(line)
                    assert len(event) <= 1 << 20
                    if line in (b"\n", b"\r\n"):
                        return bytes(event)

            opening = read_event()
            assert b"event: session.open\n" in opening, opening
            assert b"backend-session" not in opening, opening
            assert session_path.rsplit("/", 1)[1].encode() in opening, opening
            connection.send(b"pcm!" if known_length else b"4\r\npcm!\r\n")
            partial = read_event()
            assert b"partial" in partial and b"backend-session" not in partial, partial
            connection.send(b"more" if known_length else b"4\r\nmore\r\n0\r\n\r\n")
            assert b"[DONE]" in response.read()
        finally:
            connection.close()


def anthropic_event(value):
    return (
        "event: " + value["type"] + "\ndata: " + json.dumps(value) + "\n\n"
    ).encode()


def bedrock_frame(event, payload, message_type="event"):
    headers = bytearray()
    for name, value in (
        (":message-type", message_type),
        (":event-type", event),
        (":content-type", "application/json"),
    ):
        key, value = name.encode(), value.encode()
        headers += (
            bytes([len(key)]) + key + b"\x07" + struct.pack(">H", len(value)) + value
        )
    body = json.dumps(payload).encode()
    prelude = struct.pack(">II", 16 + len(headers) + len(body), len(headers))
    frame = prelude + struct.pack(">I", zlib.crc32(prelude)) + headers + body
    return frame + struct.pack(">I", zlib.crc32(frame))


def verify_hosted_image_inputs(base_url):
    """Model declarations enable new vision models without provider allowlists."""
    content = [
        {"type": "text", "text": "Compare the character sheets"},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,aW1hZ2U="}},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,c2hlZXQ="}},
    ]

    def invoke(path, body):
        request = urllib.request.Request(
            base_url + path,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)

    def assert_images(path, body):
        if path.endswith(("/converse", "/converse-stream")):
            parts = body["messages"][0]["content"]
            assert parts[0] == {"text": "Compare the character sheets"}, parts
            images = [part["image"] for part in parts if "image" in part]
            assert images == [
                {"format": "jpeg", "source": {"bytes": "aW1hZ2U="}},
                {"format": "png", "source": {"bytes": "c2hlZXQ="}},
            ], images
        elif path.endswith("/anthropic/messages"):
            parts = body["messages"][0]["content"]
            assert parts[0] == {
                "type": "text",
                "text": "Compare the character sheets",
            }, parts
            assert [part["source"] for part in parts if part["type"] == "image"] == [
                {"type": "base64", "media_type": "image/jpeg", "data": "aW1hZ2U="},
                {"type": "base64", "media_type": "image/png", "data": "c2hlZXQ="},
            ], parts
        elif path.endswith(":generateContent"):
            parts = body["contents"][0]["parts"]
            assert parts[0] == {"text": "Compare the character sheets"}, parts
            assert [part["inlineData"] for part in parts if "inlineData" in part] == [
                {"mimeType": "image/jpeg", "data": "aW1hZ2U="},
                {"mimeType": "image/png", "data": "c2hlZXQ="},
            ], parts
        else:
            assert body["messages"][0]["content"] == content, body

    for model in (
        "generators/google-known",
        "generators/vertex-known",
        "generators/bedrock-known",
        "generators/anthropic-known",
        "generators/vision-openai",
        "connections/bedrock-vision/models/new-vision-model",
        "connections/anthropic-vision/models/new-vision-model",
    ):
        body = {"model": model, "messages": [{"role": "user", "content": content}]}
        for path in (
            "/ai/v1/chat/completions",
            "/ai/v1/generate",
            "/ai/v1/generate/batch",
        ):
            before = len(Upstream.requests)
            response = invoke(
                path,
                {"requests": [{"custom_id": "sheets", "body": body}]}
                if path.endswith("/batch")
                else body,
            )
            if path.endswith("/batch"):
                assert response["summary"] == {
                    "total": 1,
                    "succeeded": 1,
                    "failed": 0,
                }, response
                response = response["data"][0]["response"]
            assert response["choices"][0]["message"]["content"], response
            assert len(Upstream.requests) == before + 1, Upstream.requests[before:]
            assert_images(*Upstream.requests[-1])
    before = len(Upstream.requests)
    response = invoke(
        "/ai/v1/chat/completions",
        {
            "model": "chains/vision-fallback",
            "messages": [{"role": "user", "content": content}],
        },
    )
    assert response["choices"][0]["message"]["content"] == "bedrock hello", response
    assert len(Upstream.requests) == before + 2, Upstream.requests[before:]
    for path, body in Upstream.requests[before:]:
        assert_images(path, body)
    for model in (
        "generators/google-known",
        "generators/vertex-known",
        "generators/bedrock-known",
        "generators/anthropic-known",
    ):
        response = invoke(
            "/ai/v1/chat/completions",
            {
                "model": model,
                "messages": [{"role": "user", "content": content}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "comparison",
                        "strict": True,
                        "schema": {
                            "type": "object",
                            "properties": {"compared": {"type": "boolean"}},
                            "required": ["compared"],
                            "additionalProperties": False,
                        },
                    },
                },
            },
        )
        assert json.loads(response["choices"][0]["message"]["content"]) == {
            "compared": True
        }, response
    # No declaration is an admission failure, not permission to silently omit
    # the images or guess from a model-name prefix. Do not call the provider.
    before = len(Upstream.requests)
    try:
        invoke(
            "/ai/v1/chat/completions",
            {
                "model": "generators/google-unknown",
                "messages": [{"role": "user", "content": content}],
            },
        )
        raise AssertionError("undeclared image input reached a provider")
    except urllib.error.HTTPError as error:
        assert error.code == 400, error.read()
    assert len(Upstream.requests) == before
    print(
        "hosted image inputs preserved across direct, batch, connection and fallback paths",
        flush=True,
    )


def verify_hosted_image_streams(base_url):
    content = [
        {"type": "text", "text": "Compare the character sheets"},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,aW1hZ2U="}},
    ]
    messages = [{"role": "user", "content": content}]
    tools = [
        {
            "type": "function",
            "function": {
                "name": "lookup",
                "parameters": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                    "required": ["id"],
                },
            },
        }
    ]
    client = openai.OpenAI(
        api_key="unused", base_url=base_url + "/ai/v1", max_retries=0, timeout=20
    )
    try:
        for model in (
            "generators/google-known",
            "generators/vertex-known",
            "generators/bedrock-known",
            "generators/anthropic-known",
            "chains/vision-fallback",
            "chains/bedrock-stream-fallback",
        ):
            for with_tools in (False, True):
                gate = threading.Event()
                Upstream.image_stream_release = gate
                try:
                    stream = client.chat.completions.create(
                        model=model,
                        messages=messages,
                        stream=True,
                        stream_options={"include_usage": True},
                        **({"tools": tools} if with_tools else {}),
                    )
                    with stream:
                        first = next(stream)
                        assert first.choices[0].delta.role == "assistant", first
                        assert not gate.is_set()
                        gate.set()
                        chunks = [first, *stream]
                    assert chunks[-1].usage.total_tokens == 5, chunks[-1]
                    if model == "generators/anthropic-known":
                        events = [
                            choice.delta.model_extra.get("extra_content", {})
                            .get("anthropic", {})
                            .get("event", {})
                            for chunk in chunks
                            for choice in chunk.choices
                        ]
                        assert any(
                            event.get("delta", {}).get("signature")
                            == "native-signature"
                            for event in events
                        ), events
                    reasons = [
                        choice.finish_reason
                        for chunk in chunks
                        for choice in chunk.choices
                        if choice.finish_reason
                    ]
                    assert reasons == ["tool_calls" if with_tools else "stop"], reasons
                    if with_tools:
                        deltas = [
                            call
                            for chunk in chunks
                            for choice in chunk.choices
                            for call in choice.delta.tool_calls or []
                        ]
                        assert {call.index for call in deltas} == {0}, deltas
                        assert json.loads(
                            "".join(call.function.arguments or "" for call in deltas)
                        ) == {"id": 7}
                        if model in (
                            "generators/google-known",
                            "generators/vertex-known",
                            "chains/bedrock-stream-fallback",
                        ):
                            assert (
                                deltas[0].model_extra["extra_content"]["google"][
                                    "thought_signature"
                                ]
                                == "opaque-signature"
                            ), deltas
                    else:
                        text = "".join(
                            choice.delta.content or ""
                            for chunk in chunks
                            for choice in chunk.choices
                        )
                        assert text in (
                            "google hello",
                            "bedrock hello",
                            "anthropic hello",
                        ), text
                finally:
                    gate.set()
                    Upstream.image_stream_release = None
        # Completed tool calls must retain opaque Google data through the
        # official OpenAI client and the next provider request.
        for model in (
            "generators/google-known",
            "generators/vertex-known",
            "generators/bedrock-known",
            "generators/anthropic-known",
        ):
            result = client.chat.completions.create(
                model=model, messages=messages, tools=tools
            )
            reply = result.choices[0].message
            assert reply.tool_calls, reply
            history = [
                *messages,
                reply.model_dump(exclude_none=True),
                {
                    "role": "tool",
                    "tool_call_id": reply.tool_calls[0].id,
                    "content": "found",
                },
            ]
            answer = client.chat.completions.create(
                model=model, messages=history, tools=tools
            )
            assert answer.choices[0].message.content in (
                "google hello",
                "bedrock hello",
                "anthropic hello",
            ), answer
            if model in ("generators/anthropic-known", "generators/bedrock-known"):
                native_provider = "anthropic" if "anthropic" in model else "bedrock"
                before = len(Upstream.requests)
                tampered = json.loads(json.dumps(history))
                tampered[1]["extra_content"][native_provider]["content"].append(
                    {
                        "type": "image",
                        "source": {
                            "type": "url",
                            "url": "https://hidden-image.invalid",
                        },
                    }
                )
                try:
                    client.chat.completions.create(
                        model=model, messages=tampered, tools=tools
                    )
                    raise AssertionError(
                        "opaque replay bypassed visible image admission"
                    )
                except openai.APIStatusError as error:
                    assert error.status_code == 400, error
                assert len(Upstream.requests) == before

        # After publication, neither native provider may replay a failure.
        for model in ("chains/bedrock-midstream", "chains/anthropic-midstream"):
            before = len(Upstream.requests)
            request = urllib.request.Request(
                base_url + "/ai/v1/chat/completions",
                data=json.dumps(
                    {
                        "model": model,
                        "messages": [{"role": "user", "content": "hello"}],
                        "stream": True,
                    }
                ).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=15) as response:
                try:
                    body = response.read()
                except http.client.IncompleteRead as error:
                    body = error.partial
            assert b"assistant" in body and b"[DONE]" not in body, body
            assert len(Upstream.requests) == before + 1, Upstream.requests[before:]
        print(
            "hosted image streams, tool metadata, early delivery and fallback commit boundaries passed",
            flush=True,
        )
    finally:
        client.close()


def verify_bedrock_stream_replay(client, messages, model="generators/bedrock-known"):
    blocks = {}
    calls = []
    stops = set()
    with client.chat.completions.create(
        model=model,
        messages=messages,
        tools=[{"type": "function", "function": {"name": "lookup"}}],
        stream=True,
    ) as stream:
        for chunk in stream:
            for choice in chunk.model_dump(exclude_none=True)["choices"]:
                delta = choice["delta"]
                metadata = delta.get("extra_content", {}).get("bedrock", {})
                event = metadata.get("event", {})
                if "contentBlockDelta" in event:
                    native = event["contentBlockDelta"]
                    reasoning = blocks.setdefault(
                        native["contentBlockIndex"], {"reasoningContent": {}}
                    )["reasoningContent"]
                    part = native["delta"]["reasoningContent"]
                    if "redactedContent" in part:
                        reasoning["redactedContent"] = base64.b64encode(
                            base64.b64decode(reasoning.get("redactedContent", ""))
                            + base64.b64decode(part["redactedContent"])
                        ).decode()
                    else:
                        text = reasoning.setdefault("reasoningText", {})
                        for key, value in part.items():
                            text[key] = text.get(key, "") + value
                if "contentBlockStop" in event:
                    stops.add(event["contentBlockStop"]["contentBlockIndex"])
                for call in delta.get("tool_calls", []):
                    if "id" in call:
                        calls.append(
                            {
                                "id": call["id"],
                                "type": "function",
                                "function": {
                                    "name": call["function"]["name"],
                                    "arguments": "",
                                },
                            }
                        )
                        blocks[metadata["content_block_index"]] = {
                            "toolUse": {
                                "toolUseId": call["id"],
                                "name": call["function"]["name"],
                                "input": None,
                            }
                        }
                    calls[call["index"]]["function"]["arguments"] += call[
                        "function"
                    ].get("arguments", "")
    assert stops == {0, 1}, stops
    if model.endswith("unsigned"):
        assert "signature" not in blocks[0]["reasoningContent"]["reasoningText"]
    for call in calls:
        block = next(
            block["toolUse"]
            for block in blocks.values()
            if block.get("toolUse", {}).get("toolUseId") == call["id"]
        )
        block["input"] = json.loads(call["function"]["arguments"])
    assistant = {
        "role": "assistant",
        "content": "",
        "tool_calls": calls,
        "extra_content": {
            "bedrock": {"content": [blocks[index] for index in sorted(blocks)]}
        },
    }
    answer = client.chat.completions.create(
        model=model,
        messages=[
            *messages,
            assistant,
            {"role": "tool", "tool_call_id": calls[0]["id"], "content": "found"},
        ],
    )
    assert answer.choices[0].message.content == "bedrock hello"


def quota_image_data_uri():
    def chunk(kind, body):
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body))
        )

    rng = random.Random(0)
    scanlines = b"".join(b"\0" + rng.randbytes(64 * 3) for _ in range(64))
    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 64, 64, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(scanlines))
        + chunk(b"IEND", b"")
    )
    return "data:image/png;base64," + base64.b64encode(png).decode()


def verify_native_replay_contracts(client, messages):
    verify_bedrock_stream_replay(client, messages, "generators/bedrock-unsigned")
    for provider, alias in (
        ("anthropic", "anthropic-known"),
        ("bedrock", "bedrock-known"),
        ("bedrock", "bedrock-unsigned"),
    ):
        model = "generators/" + alias
        result = client.chat.completions.create(model=model, messages=messages)
        reply = result.choices[0].message.model_dump(exclude_none=True)
        native = reply["extra_content"][provider]["content"]
        if alias.endswith("unsigned"):
            assert "signature" not in native[0]["reasoningContent"]["reasoningText"]
        text = reply["content"]
        for parts in (
            [{"type": "text", "text": text}],
            [
                {"type": "text", "text": ""},
                {"type": "text", "text": text[:2]},
                {"type": "text", "text": text[2:]},
                {"type": "text", "text": ""},
            ],
        ):
            multipart = {**reply, "content": parts}
            client.chat.completions.create(
                model=model,
                messages=[
                    *messages,
                    multipart,
                    {"role": "user", "content": "continue"},
                ],
            )
        for parts in (
            [{"type": "text", "text": text + "tampered"}],
            [
                {"type": "text", "text": text},
                {
                    "type": "image_url",
                    "image_url": {"url": "https://hidden.invalid/image.png"},
                },
            ],
        ):
            before = len(Upstream.requests)
            try:
                client.chat.completions.create(
                    model=model,
                    messages=[
                        *messages,
                        {**reply, "content": parts},
                        {"role": "user", "content": "continue"},
                    ],
                )
                raise AssertionError(
                    "multipart replay bypassed native projection validation"
                )
            except openai.APIStatusError as error:
                assert error.status_code == 400, error
            assert len(Upstream.requests) == before


def verify_audio_admission(client):
    output = io.BytesIO()
    with wave.open(output, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(b"\0\0" * 160)
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "media",
                    "mime_type": "audio/wav",
                    "data": base64.b64encode(output.getvalue()).decode(),
                }
            ],
        }
    ]
    for case in ("control", "image-bound", "rpm", "metered"):
        for streaming in (False, True):
            before = len(Upstream.requests)
            if case == "metered":
                try:
                    client.chat.completions.create(
                        model="generators/openai-audio-" + case,
                        messages=messages,
                        stream=streaming,
                    )
                    raise AssertionError(
                        "audio was admitted to a token quota without an audio estimate"
                    )
                except openai.APIStatusError as error:
                    assert (
                        error.status_code == 400
                        and "UnsupportedMediaTokenBudget" in str(error)
                    ), error
                assert len(Upstream.requests) == before
                continue
            result = client.chat.completions.create(
                model="generators/openai-audio-" + case,
                messages=messages,
                stream=streaming,
            )
            if streaming:
                with result as chunks:
                    assert (
                        "".join(
                            choice.delta.content or ""
                            for chunk in chunks
                            for choice in chunk.choices
                        )
                        == "google hello"
                    )
            else:
                assert result.choices[0].message.content == "google hello"
            assert len(Upstream.requests) == before + 1
            assert (
                Upstream.requests[-1][1]["messages"][0]["content"][0]["type"]
                == "input_audio"
            )


def verify_hosted_generation_contracts(base_url):
    with openai.OpenAI(
        api_key="unused", base_url=base_url + "/ai/v1", max_retries=0, timeout=30
    ) as client:
        messages = [{"role": "user", "content": "continue"}]
        verify_bedrock_stream_replay(client, messages)
        verify_native_replay_contracts(client, messages)
        verify_audio_admission(client)
        for provider in ("anthropic", "bedrock"):
            for streaming in (False, True):
                for tool in (
                    {"type": "function", "function": {"name": "lookup"}},
                    {
                        "type": "function",
                        "function": {
                            "name": "strict_lookup",
                            "strict": True,
                            "parameters": {
                                "type": "object",
                                "properties": {"id": {"type": "integer"}},
                                "required": ["id"],
                                "additionalProperties": False,
                            },
                        },
                    },
                ):
                    result = client.chat.completions.create(
                        model="generators/" + provider + "-known",
                        messages=messages,
                        tools=[tool],
                        stream=streaming,
                    )
                    if streaming:
                        with result as chunks:
                            completed = list(chunks)
                            assert any(
                                choice.delta.tool_calls
                                for chunk in completed
                                for choice in chunk.choices
                            )
                    else:
                        assert result.choices[0].message.tool_calls
        started = time.monotonic()
        try:
            client.chat.completions.create(
                model="generators/vertex-deadline",
                messages=messages,
                extra_headers={"X-Antfly-Inference-Timeout-Ms": "1000"},
            )
            raise AssertionError("credential refresh restarted the generation deadline")
        except openai.APIStatusError as error:
            assert error.status_code == 504, error
        assert time.monotonic() - started < 1.8
        for model, expected in (
            ("truncated", "length"),
            ("filtered", "content_filter"),
            ("anthropic-truncated", "length"),
            ("anthropic-filtered", "content_filter"),
        ):
            result = client.chat.completions.create(
                model="generators/" + model, messages=messages
            )
            assert result.choices[0].finish_reason == expected, result
        for model in (
            "generators/validation",
            "chains/validation",
            "generators/anthropic-validation",
            "chains/anthropic-validation",
        ):
            for streaming in (False, True):
                before = len(Upstream.requests)
                try:
                    client.chat.completions.create(
                        model=model, messages=messages, stream=streaming
                    )
                    raise AssertionError(
                        "validation error succeeded or selected a fallback"
                    )
                except openai.APIStatusError as error:
                    assert error.status_code == 400, error
                assert len(Upstream.requests) == before + 1, Upstream.requests[before:]
        before = len(Upstream.requests)
        with client.chat.completions.create(
            model="chains/anthropic-busy", messages=messages, stream=True
        ) as stream:
            assert (
                "".join(
                    choice.delta.content or ""
                    for chunk in stream
                    for choice in chunk.choices
                )
                == "google hello"
            )
        assert len(Upstream.requests) == before + 2
        for provider in ("google", "vertex", "bedrock", "anthropic", "openai"):
            images = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": quota_image_data_uri()},
                        }
                    ],
                }
            ]
            before = len(Upstream.requests)
            try:
                client.chat.completions.create(
                    model="generators/" + provider + "-quota-denied", messages=images
                )
                raise AssertionError("image reservation exceeded the bucket capacity")
            except openai.APIStatusError as error:
                assert (
                    error.status_code == 400
                    and "ProviderTokenBudgetExceeded" in str(error)
                ), error
            assert len(Upstream.requests) == before
            client.chat.completions.create(
                model="generators/" + provider + "-quota-admitted", messages=images
            )
            assert len(Upstream.requests) == before + 1
        for provider in ("google", "vertex", "bedrock", "anthropic"):
            print(f"checking long stream through {provider}", flush=True)
            text_count = 0
            finish = None
            with client.chat.completions.create(
                model="generators/" + provider + "-long",
                messages=messages,
                max_tokens=65536,
                stream=True,
            ) as stream:
                for chunk in stream:
                    for choice in chunk.choices:
                        text_count += len(choice.delta.content or "")
                        finish = choice.finish_reason or finish
            assert text_count == 40000 and finish == "length", (
                provider,
                text_count,
                finish,
            )
    print(
        "hosted generation stop reasons, terminal validation and streams beyond embedding limits passed",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--antfly",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "zig/zig-out/bin/antfly",
    )
    parser.add_argument(
        "--only-images",
        action="store_true",
        help="Run the hosted image contract without the unrelated session/discovery checks",
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
                "public_inference": {"enabled": True},
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
                "capabilities": ["models.generate", "models.decide"],
                "inference": {
                    "provider": "antfly",
                    "url": upstream_url,
                    "api_key": "mock-server-key",
                },
            }
            config["generators"]["google-known"] = {
                "provider": "gemini",
                "model": "mock-known",
                "url": upstream_url + "/google",
                "api_key": "mock-server-key",
            }
            config["generators"]["google-unknown"] = {
                "provider": "gemini",
                "model": "mock-unknown",
                "url": upstream_url + "/google",
                "api_key": "mock-server-key",
            }
            config["generators"]["vertex-known"] = {
                "provider": "vertex",
                "model": "mock-known",
                "url": upstream_url + "/google",
                "project_id": "test",
                "location": "us-central1",
                "api_key": "mock-server-key",
            }
            config["generators"]["bedrock-known"] = {
                "provider": "bedrock",
                "model": "us.anthropic.claude-next-v1:0",
                "url": upstream_url + "/bedrock",
                "region": "us-east-1",
                "api_key": "mock-server-key",
            }
            config["generators"]["anthropic-known"] = {
                "provider": "anthropic",
                "model": "future-claude",
                "url": upstream_url + "/anthropic",
                "api_key": "mock-server-key",
                "workspace_id": "fixture-workspace",
            }
            config["generators"]["google-vision-busy"] = {
                **config["generators"]["google-known"],
                "model": "mock-vision-busy",
            }
            config["generators"]["vision-openai"] = {
                "provider": "openai",
                "model": "mock-chat",
                "url": upstream_url + "/ai/v1",
                "api_key": "mock-server-key",
            }
            config["connections"]["bedrock-vision"] = {
                "kind": "inference",
                "capabilities": ["models.generate"],
                "inference": {
                    "provider": "bedrock",
                    "url": upstream_url + "/bedrock",
                    "region": "us-east-1",
                    "api_key": "mock-server-key",
                },
            }
            config["connections"]["anthropic-vision"] = {
                "kind": "inference",
                "capabilities": ["models.generate"],
                "inference": {
                    "provider": "anthropic",
                    "url": upstream_url + "/anthropic",
                    "api_key": "mock-server-key",
                    "workspace_id": "fixture-workspace",
                },
            }
            config["chains"]["vision-fallback"] = [
                {"generator": "google-vision-busy", "condition": "on_error"},
                {"generator": "bedrock-known"},
            ]
            for target in (
                "generators/google-known",
                "generators/vertex-known",
                "generators/bedrock-known",
                "generators/anthropic-known",
                "generators/vision-openai",
                "generators/google-vision-busy",
                "chains/vision-fallback",
                "connections/bedrock-vision/models/new-vision-model",
                "connections/anthropic-vision/models/new-vision-model",
            ):
                config["inference_capabilities"][target] = {
                    "images": "supported",
                    "structured_output": "supported",
                    "streaming": "supported",
                    "tools": "supported",
                }
            for name in ("stream-busy", "midstream"):
                config["generators"][name] = {
                    **config["generators"]["bedrock-known"],
                    "model": name,
                }
                config["inference_capabilities"]["generators/" + name] = {
                    "images": "supported",
                    "structured_output": "supported",
                    "streaming": "supported",
                    "tools": "supported",
                }
            for name, first in (
                ("bedrock-stream-fallback", "stream-busy"),
                ("bedrock-midstream", "midstream"),
            ):
                config["chains"][name] = [
                    {"generator": first, "condition": "on_error"},
                    {"generator": "google-known"},
                ]
                config["inference_capabilities"]["chains/" + name] = {
                    "images": "supported",
                    "structured_output": "supported",
                    "streaming": "supported",
                    "tools": "supported",
                }
            for provider in ("google", "vertex", "bedrock", "anthropic"):
                config["generators"][provider + "-long"] = {
                    **config["generators"][provider + "-known"],
                    "model": "long",
                }
                config["inference_capabilities"]["generators/" + provider + "-long"] = {
                    "streaming": "supported"
                }
            for model in ("validation", "truncated", "filtered"):
                config["generators"][model] = {
                    **config["generators"]["bedrock-known"],
                    "model": model,
                }
                config["inference_capabilities"]["generators/" + model] = {
                    "streaming": "supported"
                }
            config["chains"]["validation"] = [
                {"generator": "validation", "condition": "on_error"},
                {"generator": "google-known"},
            ]
            config["inference_capabilities"]["chains/validation"] = {
                "streaming": "supported"
            }
            for suffix in ("validation", "busy", "midstream", "truncated", "filtered"):
                name = "anthropic-" + suffix
                config["generators"][name] = {
                    **config["generators"]["anthropic-known"],
                    "model": name,
                }
                config["inference_capabilities"]["generators/" + name] = {
                    "streaming": "supported"
                }
            for suffix in ("validation", "busy", "midstream"):
                name = "anthropic-" + suffix
                config["chains"][name] = [
                    {"generator": name, "condition": "on_error"},
                    {"generator": "google-known"},
                ]
                config["inference_capabilities"]["chains/" + name] = {
                    "streaming": "supported"
                }
            for provider in ("google", "vertex", "bedrock", "anthropic"):
                for case, capacity in (("denied", 1000), ("admitted", 10000)):
                    name = provider + "-quota-" + case
                    config["generators"][name] = {
                        **config["generators"][provider + "-known"],
                        "model": name,
                        "max_tokens": 16,
                        "max_image_input_tokens": 2000,
                        "rate_limit": {"tokens_per_minute": capacity},
                    }
                    config["inference_capabilities"]["generators/" + name] = {
                        "images": "supported"
                    }
            config["generators"]["openai-known"] = dict(
                config["generators"]["vision-openai"]
            )
            for case, capacity in (("denied", 1000), ("admitted", 10000)):
                name = "openai-quota-" + case
                config["generators"][name] = {
                    **config["generators"]["openai-known"],
                    "model": name,
                    "max_tokens": 16,
                    "max_image_input_tokens": 2000,
                    "rate_limit": {"tokens_per_minute": capacity},
                }
                config["inference_capabilities"]["generators/" + name] = {
                    "images": "supported"
                }
            config["generators"]["bedrock-unsigned"] = {
                **config["generators"]["bedrock-known"],
                "model": "unsigned",
            }
            config["inference_capabilities"]["generators/bedrock-unsigned"] = {
                "tools": "supported",
                "streaming": "supported",
                "images": "supported",
            }
            for case in ("control", "image-bound", "rpm", "metered"):
                name = "openai-audio-" + case
                generator = {**config["generators"]["openai-known"], "model": name}
                if case != "control":
                    generator["max_image_input_tokens"] = 2000
                if case == "rpm":
                    generator["rate_limit"] = {"requests_per_minute": 600, "burst": 10}
                elif case == "metered":
                    generator["rate_limit"] = {"tokens_per_minute": 10000}
                config["generators"][name] = generator
                config["inference_capabilities"]["generators/" + name] = {
                    "audio": "supported",
                    "streaming": "supported",
                }
            credentials = root / "mock-adc.json"
            credentials.write_text(
                json.dumps(
                    {
                        "type": "authorized_user",
                        "client_id": "mock",
                        "client_secret": "mock",
                        "refresh_token": "mock",
                        "token_uri": upstream_url + "/mock-token",
                    }
                )
            )
            vertex = dict(config["generators"]["vertex-known"])
            del vertex["api_key"]
            vertex.update(model="deadline-model", credentials_path=str(credentials))
            config["generators"]["vertex-deadline"] = vertex
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
                    verify_hosted_image_inputs(base_url)
                    verify_hosted_image_streams(base_url)
                    verify_hosted_generation_contracts(base_url)
                    if args.only_images:
                        return
                    Upstream.model_delay_seconds = 0.3
                    started = time.monotonic()
                    try:
                        urllib.request.urlopen(
                            urllib.request.Request(
                                base_url + "/ai/v1/models",
                                headers={"X-Antfly-Inference-Timeout-Ms": "20"},
                            ),
                            timeout=2,
                        )
                        raise AssertionError("cold discovery ignored caller deadline")
                    except urllib.error.HTTPError as error:
                        assert error.code == 504, error.read()
                    finally:
                        Upstream.model_delay_seconds = 0
                    assert time.monotonic() - started < 1
                    # A caller timeout must not leave a provider failure in cache.
                    with urllib.request.urlopen(
                        base_url + "/ai/v1/models", timeout=10
                    ) as response:
                        catalog = json.load(response)
                    physical = {item["id"]: item for item in catalog["data"]}
                    assert (
                        physical["connections/anthropic-vision/models/future-claude"][
                            "task"
                        ]
                        == "generate"
                    )
                    assert (
                        physical["connections/runtime/models/mock-sparse"]["task"]
                        == "embed"
                    )
                    assert physical["connections/runtime/models/mock-sparse"][
                        "tasks"
                    ] == ["embed"]
                    assert (
                        physical["connections/runtime/models/mock-rerank"]["task"]
                        == "rerank"
                    )
                    assert (
                        physical["connections/runtime/models/mock-overflow"]["task"]
                        == "embed"
                    )
                    assert (
                        physical["connections/runtime/models/mock-huge"]["task"]
                        == "embed"
                    )
                    decision_id = "connections/runtime/models/mock-decision"
                    assert physical[decision_id]["task"] == "decide"
                    assert physical[decision_id]["tasks"] == ["decide"]
                    assert decision_id in catalog["deciders"]
                    decision_request = urllib.request.Request(
                        base_url + "/ai/v1/decisions",
                        data=json.dumps(
                            {
                                "model": decision_id,
                                "input": "refund",
                                "questions": [
                                    {
                                        "name": "refund",
                                        "type": "predicate",
                                        "instructions": "Refund?",
                                    }
                                ],
                            }
                        ).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(
                        decision_request, timeout=5
                    ) as response:
                        assert json.load(response)["answers"][0]["probability"] == 0.9
                    data_only = physical["connections/runtime/models/mock-data-only"]
                    assert set(data_only["tasks"]) == {"generate", "embed"}, data_only
                    assert "task" not in data_only
                    shared = physical["connections/runtime/models/mock-shared"]
                    assert set(shared["tasks"]) == {"generate", "embed"}
                    assert "task" not in shared
                    assert catalog["predictors"]["predictors/risk"] == {
                        "task": "regression",
                        "num_features": 2,
                        "num_outputs": 1,
                        "feature_names": ["first", "second"],
                    }

                    # A successful HTTP response is still untrusted protocol data.
                    # Malformed session envelopes must fail without killing the runtime.
                    for malformed in (
                        [],
                        None,
                        "session",
                        42,
                        {},
                        {"id": None},
                        {"id": ""},
                        {"id": ".."},
                        {"id": "first", "session_id": "second"},
                        {"id": "expired", "expires_at": int(time.time()) - 10},
                        {"id": "expired", "expires_at": int(time.time())},
                        {"id": "invalid", "expires_at": None},
                        {"id": "invalid", "expires_at": "future"},
                        b"{",
                    ):
                        Upstream.session_reply_override = True
                        Upstream.session_reply = malformed
                        try:
                            request = urllib.request.Request(
                                base_url + "/ai/v1/transcription/sessions",
                                data=json.dumps(
                                    {"model": "transcribers/speech"}
                                ).encode(),
                                headers={"Content-Type": "application/json"},
                            )
                            try:
                                urllib.request.urlopen(request, timeout=5)
                                raise AssertionError(
                                    "malformed session response accepted"
                                )
                            except urllib.error.HTTPError as error:
                                assert error.code == 502, error.read()
                                assert b"InvalidInferenceResponse" in error.read()
                        finally:
                            Upstream.session_reply_override = False
                        with urllib.request.urlopen(
                            base_url + "/readyz", timeout=2
                        ) as response:
                            assert response.status == 200

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
                            stream_options={"include_usage": True},
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
                            chunks = list(stream)
                            arguments = "".join(
                                chunk.choices[0].delta.tool_calls[0].function.arguments
                                or ""
                                for chunk in chunks
                                if chunk.choices
                            )
                            assert chunks[-1].usage.total_tokens == 5
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
                    # Creation latency must not consume the backend's remaining
                    # lifetime before the public handle can be used.
                    for malformed_response in (
                        "malformed-batch-response",
                        "error-batch-response",
                        "incomplete-batch-response",
                    ):
                        malformed_batch = invoke(
                            "/ai/v1/generate/batch",
                            {
                                "requests": [
                                    {
                                        "custom_id": "malformed",
                                        "body": {
                                            "model": "generators/support",
                                            "messages": [
                                                {
                                                    "role": "user",
                                                    "content": malformed_response,
                                                }
                                            ],
                                        },
                                    },
                                    {
                                        "custom_id": "valid",
                                        "body": {
                                            "model": "generators/support",
                                            "messages": [
                                                {"role": "user", "content": "hello"}
                                            ],
                                        },
                                    },
                                ]
                            },
                        )
                        assert malformed_batch["summary"] == {
                            "total": 2,
                            "succeeded": 1,
                            "failed": 1,
                        }, malformed_batch
                        assert [
                            item["custom_id"] for item in malformed_batch["data"]
                        ] == [
                            "malformed",
                            "valid",
                        ]
                        assert (
                            malformed_batch["data"][0]["error"]["code"]
                            == "InvalidInferenceResponse"
                        ), malformed_batch
                        assert (
                            malformed_batch["data"][1]["response"]["choices"][0][
                                "message"
                            ]["content"]
                            == "hello"
                        ), malformed_batch
                        assert malformed_batch["data"][1]["response"][
                            "provider_extension"
                        ] == {"preserved": True}
                        assert (
                            malformed_batch["data"][1]["response"]["usage"][
                                "provider_tokens"
                            ]
                            == 7
                        )
                    # Canonical providers and opaque connections share the
                    # same response contract, including unavailable usage.
                    for name in ("google-known", "vertex-known", "google-unknown"):
                        body = {
                            "model": "generators/" + name,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                        single = invoke("/ai/v1/generate", body)
                        batch = invoke(
                            "/ai/v1/generate/batch",
                            {"requests": [{"custom_id": name, "body": body}]},
                        )
                        assert batch["summary"] == {
                            "total": 1,
                            "succeeded": 1,
                            "failed": 0,
                        }, batch
                        for response in (single, batch["data"][0]["response"]):
                            assert (
                                response["choices"][0]["message"]["content"]
                                == "google hello"
                            ), response
                            assert response["usage"] == (
                                None
                                if name.endswith("unknown")
                                else {
                                    "prompt_tokens": 2,
                                    "completion_tokens": 1,
                                    "total_tokens": 5,
                                }
                            ), response
                    Upstream.session_idle_ttl = 2
                    Upstream.session_append_delay = 3
                    try:
                        for action in ("audio", "stream"):
                            active = invoke(
                                "/ai/v1/transcription/sessions",
                                {"model": "transcribers/speech", "ttl_seconds": 2},
                            )
                            active_path = (
                                "/ai/v1/transcription/sessions/" + active["id"]
                            )
                            if action == "audio":
                                invoke(active_path + "/audio", {"audio": "aGk="})
                            else:
                                stream_request = urllib.request.Request(
                                    base_url
                                    + active_path
                                    + "/stream?format=pcm_f32&sample_rate=48000&commit=false",
                                    data=b"pcm!more",
                                    headers={
                                        "Content-Type": "application/octet-stream"
                                    },
                                )
                                with urllib.request.urlopen(
                                    stream_request, timeout=10
                                ) as response:
                                    assert b"[DONE]" in response.read()
                            assert (
                                invoke(active_path, method="GET")["id"] == active["id"]
                            )
                            invoke(active_path, method="DELETE")
                    finally:
                        Upstream.session_idle_ttl = 0
                        Upstream.session_append_delay = 0
                    Upstream.session_creation_delay = 3
                    try:
                        short_session = invoke(
                            "/ai/v1/transcription/sessions",
                            {"model": "transcribers/speech"},
                        )
                    finally:
                        Upstream.session_creation_delay = 0
                    short_path = "/ai/v1/transcription/sessions/" + short_session["id"]
                    assert invoke(short_path, method="GET")["id"] == short_session["id"]
                    invoke(short_path, method="DELETE")
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
                    verify_session_transports(base_url, session_path)
                    invoke(session_path, method="DELETE")
                    # Terminal delete outcomes retire ownership; transient failures
                    # preserve it so clients can retry against the same backend.
                    for delete_status in (204, 404, 410, 503):
                        deletion_session = invoke(
                            "/ai/v1/transcription/sessions",
                            {"model": "transcribers/speech"},
                        )
                        deletion_path = (
                            "/ai/v1/transcription/sessions/" + deletion_session["id"]
                        )
                        Upstream.session_delete_status = delete_status
                        try:
                            try:
                                invoke(deletion_path, method="DELETE")
                                assert delete_status == 204
                            except urllib.error.HTTPError as error:
                                assert error.code == delete_status, error.read()
                            gets_before = Upstream.session_gets
                            if delete_status == 503:
                                assert (
                                    invoke(deletion_path, method="GET")["id"]
                                    == deletion_session["id"]
                                )
                                assert Upstream.session_gets == gets_before + 1
                            else:
                                try:
                                    invoke(deletion_path, method="GET")
                                    raise AssertionError(
                                        "deleted affinity still usable"
                                    )
                                except urllib.error.HTTPError as error:
                                    assert error.code == 404, error.read()
                                assert Upstream.session_gets == gets_before
                        finally:
                            Upstream.session_delete_status = 204
                        if delete_status == 503:
                            invoke(deletion_path, method="DELETE")
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
                    telemetry = []
                    for line in (root / "server.log").read_text().splitlines():
                        if "invocation {" in line:
                            telemetry.append(
                                json.loads(line.split("invocation ", 1)[1])
                            )
                    assert any(
                        item["requested_alias"] == "generators/support"
                        and item["time_to_first_output_ns"] is not None
                        and item["input_tokens"] == 2
                        and item["output_tokens"] == 1
                        and item["total_tokens"] == 5
                        for item in telemetry
                    ), telemetry
                    verify_nested_gateway(args.antfly, root, base_url)
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
