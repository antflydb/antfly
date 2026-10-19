# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
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

"""Grouped generation, cross-shard fan-in, and finite prior-context replay."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
import requests

from helpers import start_http_server, wait_until


@pytest.mark.parametrize(
    ("raft", "relational", "max_members", "external_vector"),
    [
        (False, False, 16, False),
        (True, False, 16, False),
        (True, True, 16, False),
        (True, False, 1, False),
        (True, False, 16, True),
    ],
    ids=[
        "standalone",
        "raft",
        "raft-relational",
        "raft-multiple-units",
        "raft-external-vector",
    ],
)
def test_grouped_generation_orders_prior_context_and_bounds_replay(
    request, monkeypatch, raft, relational, max_members, external_vector
):
    if raft:
        monkeypatch.setenv("ANTFLY_E2E_STATEFUL_RAFT", "1")
    if max_members == 1:
        monkeypatch.setenv("ANTFLY_ENRICHMENT_PREPARATION_WINDOW_ITEMS", "1")
    api = request.getfixturevalue("stateful_api")
    calls: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            try:
                request = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                assert request["tool_choice"]["function"]["name"] == "antfly_output"
                prompt = json.loads(request["messages"][1]["content"])
                items = []
                for item in prompt["items"]:
                    text = item["input"]
                    with lock:
                        calls.append(text)
                    turn = (
                        "c"
                        if text.startswith("Turn C")
                        else "b"
                        if text.startswith("Turn B")
                        else "a"
                    )
                    claim = f"claim-{turn}"
                    if turn == "a" and text.startswith("beta"):
                        claim = "claim-a-beta"
                    if turn == "a" and "Amended" in text:
                        claim = "claim-a-v2"
                    prior = [
                        name
                        for name in ("claim-a-v2", "claim-a", "claim-b")
                        if name in text
                    ]
                    if "claim-a-v2" in prior:
                        prior = [name for name in prior if name != "claim-a"]
                    items.append(
                        {
                            "item": item["id"],
                            "value": {
                                "id": claim,
                                "kind": "claim",
                                "statement": f"statement-{claim}",
                                "replaces": prior,
                            },
                        }
                    )
                payload = {
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
                self.reply(200, payload)
            except Exception as exc:
                errors.append(exc)
                self.reply(500, {"error": repr(exc)})

        def reply(self, status, value):
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    table = f"grouped_{uuid4().hex[:8]}"
    try:
        api.create_table(table, num_shards=2)
        if relational:
            api.update_schema(
                table,
                {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "thread_id": {"type": "string"},
                                    "turn_id": {"type": "string"},
                                    "ordinal": {"type": "integer"},
                                    "speaker": {"type": "string"},
                                    "text": {"type": "string"},
                                },
                                "required": [
                                    "thread_id",
                                    "turn_id",
                                    "ordinal",
                                    "speaker",
                                    "text",
                                ],
                                "additionalProperties": False,
                            }
                        }
                    },
                },
            )
        if external_vector:
            # Physical vector-index convergence is independent of grouped
            # producer sequencing and cannot hold up later conversation turns.
            api.create_index(
                table,
                "authored_vectors",
                {"type": "embeddings", "external": True, "dimension": 3},
            )
        api.put(
            f"/tables/{table}/artifacts/turns/enrichment",
            {
                "kind": "group",
                "source": {
                    "group_by": "thread_id",
                    "order_by": "ordinal",
                    "segment_by": "turn_id",
                    "field": "text",
                    "filter": {"term": {"speaker": ["user", "assistant"]}},
                    "max_bytes": 8192,
                    "max_members": max_members,
                },
            },
        )
        api.put(
            f"/tables/{table}/artifacts/claims/enrichment",
            {
                "kind": "asset",
                "source_artifact_name": "turns",
                "content_type": "application/json",
                "producer": {
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
                                "id": {"type": "string"},
                                "kind": {"type": "string"},
                                "statement": {"type": "string"},
                                "replaces": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                            "required": ["id", "kind", "statement", "replaces"],
                            "additionalProperties": False,
                        },
                    },
                },
                "prior_context": {
                    "artifact": "claims",
                    "render": "{{id}}: {{statement}}",
                    "max_items": 4,
                    "max_bytes": 2048,
                    "horizon_segments": 1,
                },
            },
        )

        api.create_index(
            table,
            "claims_graph",
            {
                "type": "graph",
                "source": {
                    "artifact": "claims",
                    "format": "extraction_relation",
                    "nodes": {
                        "source": "thread/{{_doc.value._group_key}}",
                        "target": "{{_item.id}}",
                    },
                    "edge": {
                        "type": "about",
                        "edge_id": "{{_doc.key}}:{{_item.id}}",
                        "metadata": {
                            "unit": "{{_doc.key}}",
                            "segment": "{{_doc.value._segment_key}}",
                        },
                    },
                    "context": {"doc_fields": ["_group_key", "_segment_key"]},
                },
                "edge_types": [{"name": "about"}],
            },
        )

        def graph_nodes(group="thread"):
            result = api.query_table(
                table,
                {
                    "graph_queries": {
                        "claims": {
                            "index": "claims_graph",
                            "traverse": {
                                "start": {"keys": [f"thread/{group}"]},
                                "edge_types": ["about"],
                                "max_depth": 1,
                            },
                        }
                    },
                    "limit": 10,
                },
            )
            responses = result.get("responses", [])
            if not responses:
                return set()
            graph = responses[0].get("graph_results", {}).get("claims", {})
            return {node["key"] for node in graph.get("nodes", [])}

        def row(text, ordinal, turn, speaker="user"):
            return {
                "thread_id": "thread",
                "turn_id": turn,
                "ordinal": ordinal,
                "speaker": speaker,
                "text": text,
            }

        def write(table_name, **kwargs):
            # A committed 202 is an explicit pending visibility outcome. Retry
            # this idempotent fixture mutation until its barrier is satisfied.
            observed = {}

            def attempt():
                nonlocal observed
                observed = api.batch_write_with_timeout(
                    table_name, timeout_s=90, **kwargs
                )
                assert observed.get("status") != "committed_repair_required", (
                    observed,
                    api.debug_logs()[-16000:],
                )
                return observed.get("status") != "committed_pending"

            assert wait_until(attempt, timeout_s=120, interval_s=0.1), (
                observed,
                api.debug_logs()[-16000:],
            )
            return observed

        def matching(prefix):
            with lock:
                return [text for text in calls if text.startswith(prefix)]

        # Bad membership must fail before a durable Raft entry, so the next
        # valid write also verifies that rejection did not poison owner apply.
        for invalid in (
            row("oversized" * 1025, 0, "invalid"),
            row("bad", "bad", "invalid"),
        ):
            with pytest.raises(requests.HTTPError) as rejected:
                api.batch_write(table, inserts={"invalid": invalid}, sync_level="write")
            assert rejected.value.response.status_code == 400, rejected.value

        write(
            table,
            inserts={
                "0001": row("Turn A alpha", 1, "a"),
                "ff02": row("beta", 2, "a", "assistant"),
                "tool": row("ignored tool output", 2, "a", "tool"),
            },
            sync_level="write",
        )
        assert wait_until(
            lambda: (
                matching("Turn A") and matching("beta")
                if max_members == 1
                else [text for text in matching("Turn A") if "beta" in text]
            ),
            timeout_s=60,
            interval_s=0.1,
        ), api.debug_logs()[-16000:]
        write(
            table,
            inserts={"0003": row("Turn B current", 3, "b")},
            sync_level="enrichments",
        )
        b = matching("Turn B")
        assert b and "claim-a" in b[-1], api.debug_logs()[-16000:]
        assert "ignored tool output" not in "\n".join(calls)
        write(
            table,
            inserts={"ff04": row("Turn C current", 4, "c")},
            sync_level="enrichments",
        )
        c = matching("Turn C")
        assert c and "claim-b" in c[-1] and "claim-a" not in c[-1]
        c_count = len(c)
        write(
            table,
            inserts={"0001": row("Turn A Amended alpha", 1, "a")},
            sync_level="enrichments",
        )
        assert any("claim-a-v2" in text for text in matching("Turn B"))
        # The unchanged source retry is also a visibility fence. Completing B
        # must never extend the original A event's one-segment horizon to C.
        write(
            table,
            inserts={"0001": row("Turn A Amended alpha", 1, "a")},
            sync_level="enrichments",
        )
        assert len(matching("Turn C")) == c_count
        assert wait_until(
            lambda: {"claim-a-v2", "claim-b", "claim-c"} <= graph_nodes(),
            timeout_s=60,
            interval_s=0.1,
        ), api.debug_logs()[-16000:]
        assert "claim-a" not in graph_nodes()

        # Readiness and producer receipts survive a real server restart. A
        # visibility retry of identical source content must not call the model.
        with lock:
            before_restart = list(calls)
        api.restart_server()
        write(
            table,
            inserts={"0001": row("Turn A Amended alpha", 1, "a")},
            sync_level="enrichments",
        )
        with lock:
            assert calls == before_restart, calls

        b_calls_before_retirement = len(matching("Turn B"))
        write(table, deletes=["0003"], sync_level="enrichments")
        assert wait_until(
            lambda: "claim-b" not in graph_nodes(), timeout_s=60, interval_s=0.1
        ), api.debug_logs()[-16000:]
        assert len(matching("Turn B")) == b_calls_before_retirement
        moved = row("Turn C current", 4, "c")
        moved["thread_id"] = "other"
        write(table, inserts={"ff04": moved}, sync_level="enrichments")
        assert wait_until(
            lambda: (
                "claim-c" not in graph_nodes() and "claim-c" in graph_nodes("other")
            ),
            timeout_s=60,
            interval_s=0.1,
        ), api.debug_logs()[-16000:]
        write(table, deletes=["0001", "ff02"], sync_level="enrichments")
        assert wait_until(
            lambda: not (graph_nodes() & {"claim-a-v2", "claim-b", "claim-c"}),
            timeout_s=60,
            interval_s=0.1,
        ), api.debug_logs()[-16000:]
        assert "claim-a-beta" not in graph_nodes()
        assert not errors, errors
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
