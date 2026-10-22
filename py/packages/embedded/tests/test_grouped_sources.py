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

"""Grouped producers advance through the public embedded API without waiting writes."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import antfly_embedded as af


@pytest.mark.parametrize("storage", [af.Storage.LITE, af.Storage.DIRECTORY])
@pytest.mark.parametrize("child", [False, True], ids=["root", "table"])
@pytest.mark.parametrize("drive", [False, True], ids=["background", "idle"])
def test_grouped_sources_drive_downstream_graph_without_waiting_write(require_native, tmp_path, storage, child, drive):
    inputs = []
    errors = []

    class Generator(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                prompt = json.loads(request["messages"][1]["content"])
                items = []
                for item in prompt["items"]:
                    text = item["input"]
                    inputs.append(text)
                    name = "Carol" if "turn-two" in text else "Ada"
                    items.append(
                        {
                            "item": item["id"],
                            "value": {
                                "entities": [{"id": "person", "text": name, "label": "concept"}],
                                "relations": [],
                            },
                        }
                    )
                response = {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "id": "group-output",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ]
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 10},
                }
                raw = json.dumps(response).encode()
                self.send_response(200)
            except Exception as exc:
                errors.append(exc)
                raw = json.dumps({"error": repr(exc)}).encode()
                self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Generator)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        options = af.OpenOptions(storage=storage, profile=af.Profile.NATIVE, generated_enrichment_replay=True)
        path = tmp_path / ("groups.aflite" if storage == af.Storage.LITE else "groups")
        with af.create_with_options(path, options) as db:
            db.create_table("concepts", {})
            if child:
                db.create_table("history", {})
            table = db.open_table("history") if child else db
            try:
                table.add_enrichment(
                    {
                        "name": "turns",
                        "kind": "group",
                        "source": {
                            "group_by": "thread_id",
                            "order_by": "ordinal",
                            "segment_by": "turn_id",
                            "field": "text",
                            "filter": {"term": {"speaker": ["user", "assistant"]}},
                        },
                    }
                )
                table.add_enrichment(
                    {
                        "name": "turn_concepts",
                        "kind": "asset",
                        "source_artifact_name": "turns",
                        "content_type": "application/json",
                        "producer_json": json.dumps(
                            {
                                "type": "generator",
                                "config": {
                                    "generator": {
                                        "provider": "openai",
                                        "api_key": "fixture",
                                        "model": "gpt-4.1",
                                        "url": f"http://127.0.0.1:{server.server_port}/v1",
                                    },
                                    "output_schema": {
                                        "type": "object",
                                        "properties": {
                                            "entities": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": {"type": "string"},
                                                        "text": {"type": "string"},
                                                        "label": {"type": "string"},
                                                    },
                                                    "required": ["id", "text", "label"],
                                                    "additionalProperties": False,
                                                },
                                            },
                                            "relations": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "source": {"type": "string"},
                                                        "target": {"type": "string"},
                                                        "type": {"type": "string"},
                                                    },
                                                    "required": ["source", "target", "type"],
                                                    "additionalProperties": False,
                                                },
                                            },
                                        },
                                        "required": ["entities", "relations"],
                                        "additionalProperties": False,
                                    },
                                },
                            }
                        ),
                    }
                )
                table.add_index(
                    {
                        "name": "turn_graph",
                        "kind": "graph",
                        "source": {
                            "artifact": "turn_concepts",
                            "format": "extraction_relation",
                            "path": "$.relations[*]",
                            "mention_edge_type": "about",
                            "nodes": {"source": "thread/{{_doc.value._group_key}}", "target": "{{_item.target}}"},
                            "edge": {"type": "{{ _item.type }}", "edge_id": "{{_doc.key}}:{{_item.type}}"},
                            "context": {"doc_fields": ["_group_key", "_segment_key"]},
                        },
                        "edge_types": [{"name": "about"}],
                        "resolvers": [
                            {
                                "name": "concepts",
                                "table": "concepts",
                                "promote": False,
                                "source_artifact": "turn_concepts",
                                "resolution_artifact": "resolved",
                                "key_template": "concept/{{ uslug _entity.text }}",
                                "config_generation": 1,
                            }
                        ],
                    }
                )
                rows = {
                    "one": {"thread_id": "t1", "turn_id": "u1", "ordinal": 1, "speaker": "user", "text": "turn-one"},
                    "two": {"thread_id": "t1", "turn_id": "u1", "ordinal": 2, "speaker": "assistant", "text": "answer"},
                    "tool": {
                        "thread_id": "t1",
                        "turn_id": "u1",
                        "ordinal": 3,
                        "speaker": "tool",
                        "text": "excluded-tool",
                    },
                    "three": {"thread_id": "t1", "turn_id": "u2", "ordinal": 4, "speaker": "user", "text": "turn-two"},
                }
                table.batch_json({"inserts": rows, "sync_level": "write"})
                if drive:
                    table.run_until_idle()
                deadline = time.monotonic() + (0 if drive else 30)
                edges = table.edges("turn_graph", "thread/t1", "about", 0)
                while time.monotonic() < deadline:
                    edges = table.edges("turn_graph", "thread/t1", "about", 0)
                    if len(edges) == 2:
                        break
                    time.sleep(0.02)
                assert not errors, errors
                assert len(edges) == 2, (edges, inputs, table.pending_work_stats())
                assert any("turn-one" in text and "answer" in text for text in inputs), inputs
                assert any("turn-two" in text for text in inputs), inputs
                assert all("excluded-tool" not in text for text in inputs), inputs
            finally:
                if child:
                    table.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
