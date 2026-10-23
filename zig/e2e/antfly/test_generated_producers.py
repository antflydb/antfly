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

"""Constrained producer publication and replay over the public API."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest

from helpers import start_http_server, wait_until


@pytest.mark.parametrize("kind", ["generator", "extractor", "decision"])
def test_constrained_producer_publishes_valid_artifacts_and_reuses_replay(
    stateful_api, kind
):
    calls = []
    fixture_errors = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            try:
                self.reply()
            except Exception as exc:
                fixture_errors.append(exc)
                payload = repr(exc).encode()
                self.send_response(500)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        def reply(self):
            assert self.path == "/v1/chat/completions"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert body["tool_choice"]["function"]["name"] == "antfly_output"
            assert body["tools"][0]["function"]["strict"] is True
            prompt = json.loads(body["messages"][1]["content"])
            with lock:
                calls.append(prompt)
                count = len(calls)
            items = []
            for item in prompt["items"]:
                value = (
                    {"kind": "fact", "statement": item["input"]}
                    if kind == "generator"
                    else {"answer": "same"}
                    if kind == "decision"
                    else {
                        "entities": [
                            {"label": "person", "text": "Éva", "present": True},
                            {"label": "person", "text": "Ghost", "present": True},
                        ],
                        "relations": [],
                    }
                )
                if kind == "generator" and count == 1:
                    value.pop("statement")
                items.append({"item": item["id"], "value": value})
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": f"call_{count}",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ],
                            }
                        }
                    ],
                    "usage": {"prompt_tokens": 20, "completion_tokens": 10},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    try:
        table = f"generated_{uuid4().hex[:8]}"
        stateful_api.create_table(table)
        config = {
            "generator": {
                "provider": "openai",
                "api_key": "fixture-key",
                "model": "gpt-4.1",
                "url": f"http://127.0.0.1:{server.server_port}/v1",
            },
            "output": "auto",
            "batch": {"max_items_per_call": 8},
        }
        if kind == "generator":
            config["output_schema"] = {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["fact"]},
                    "statement": {"type": "string"},
                },
                "required": ["kind", "statement"],
                "additionalProperties": False,
            }
        elif kind == "decision":
            config = {
                "version": "test-generated-v1",
                "decider": {
                    **config,
                    "decision_method": "generated",
                    "probabilities": "verdict_only",
                    "instructions": "Compare the concepts.",
                },
                "questions": [
                    {
                        "type": "choice",
                        "name": "answer",
                        "instructions": "Choose the relation.",
                        "choices": [
                            {"value": "same", "description": "Same concept"},
                            {"value": "different", "description": "Distinct concepts"},
                        ],
                    }
                ],
            }
        else:
            config.update(
                method="generated",
                probabilities="verdict_only",
                schema={"entities": ["person"], "relations": []},
            )
        stateful_api.put(
            f"/tables/{table}/artifacts/derived/enrichment",
            {
                "kind": "asset",
                "field": "body",
                "content_type": "application/json",
                "producer": {"type": kind, "config": config},
            },
        )
        stateful_api.batch_write(
            table, inserts={"doc": {"body": "Éva met Éva."}}, sync_level="enrichments"
        )

        def artifact():
            row = stateful_api.get(
                f"/tables/{table}/documents/doc?fields=_artifacts.derived"
            )
            return row.get("_artifacts", {}).get("derived", {}).get("value")

        output = wait_until(artifact, timeout_s=30, interval_s=0.1)
        assert not fixture_errors, fixture_errors
        assert output is not None
        if isinstance(output, str):
            output = json.loads(output)
        if kind == "generator":
            assert output == {"kind": "fact", "statement": "Éva met Éva."}
            assert len(calls) == 2
            assert calls[1]["validation_error"]
        elif kind == "decision":
            answer = output["answers"][0]
            assert answer["choice"] == "same"
            assert answer["raw_structured_answer"] == "same"
            assert answer["probability_source"] == "verdict"
            assert output["provenance"]["provider"] == "openai"
            assert output["provenance"]["specification_hash"]
        else:
            assert [entity["start"] for entity in output["entities"]] == [0, 9]
            assert output["unaligned_mentions"] == 1
            assert output["probability_source"] == "verdict"
        count = len(calls)
        stateful_api.batch_write(
            table, inserts={"doc": {"body": "Éva met Éva."}}, sync_level="enrichments"
        )
        assert artifact()
        assert len(calls) == count
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
