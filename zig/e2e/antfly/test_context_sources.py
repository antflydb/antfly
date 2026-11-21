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

"""Cross-table context sources on multi-shard tables over the public API.

The producer echoes its rendered input, so each artifact shows exactly which
context it was built from. Both tables span several Raft groups, so source
notices cross groups and tables, resolution uses the routed read path, and
rebuilds come from the leader-only maintenance job.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
import requests
from helpers import start_http_server, wait_until

NUM_SHARDS = 3


class EchoGenerator:
    """OpenAI-compatible structured generator echoing each item's input."""

    def __init__(self) -> None:
        self.inputs: list[str] = []
        self.errors: list[Exception] = []
        self.lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                try:
                    payload = outer.reply(
                        json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    )
                    self.send_response(200)
                except Exception as exc:  # noqa: BLE001 - surfaced by the test
                    outer.errors.append(exc)
                    payload = repr(exc).encode()
                    self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = start_http_server(self.server)
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1"

    def reply(self, body: dict) -> bytes:
        prompt = json.loads(body["messages"][1]["content"])
        items = []
        with self.lock:
            for item in prompt["items"]:
                self.inputs.append(item["input"])
                items.append(
                    {
                        "item": item["id"],
                        "value": {"kind": "fact", "statement": item["input"]},
                    }
                )
        call = {
            "id": f"call_{len(self.inputs)}",
            "type": "function",
            "function": {
                "name": "antfly_output",
                "arguments": json.dumps({"items": items}),
            },
        }
        return json.dumps(
            {
                "choices": [{"message": {"content": None, "tool_calls": [call]}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 10},
            }
        ).encode()

    def calls_for(self, needle: str) -> int:
        with self.lock:
            return sum(needle in value for value in self.inputs)

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.fixture
def echo_generator():
    generator = EchoGenerator()
    yield generator
    generator.stop()


def _enrichment(generator: EchoGenerator, context_sources: dict) -> dict:
    return {
        "kind": "asset",
        "field": "body",
        "content_type": "application/json",
        "producer": {
            "type": "generator",
            "config": {
                "generator": {
                    "provider": "openai",
                    "api_key": "fixture-key",
                    "model": "gpt-4.1",
                    "url": generator.url,
                },
                "output": "auto",
                "batch": {"max_items_per_call": 8},
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": ["fact"]},
                        "statement": {"type": "string"},
                    },
                    "required": ["kind", "statement"],
                    "additionalProperties": False,
                },
            },
        },
        "context_sources": context_sources,
    }


def _statement(api, table: str, key: str, artifact: str) -> str | None:
    try:
        row = api.get(f"/tables/{table}/documents/{key}?fields=_artifacts.{artifact}")
    except Exception:  # noqa: BLE001 - polling until the artifact exists
        return None
    value = row.get("_artifacts", {}).get(artifact, {}).get("value")
    if isinstance(value, str):
        value = json.loads(value)
    return value.get("statement") if isinstance(value, dict) else None


def _statement_containing(api, table, key, artifact, *needles, absent=()):
    def ready():
        statement = _statement(api, table, key, artifact)
        if statement is None:
            return None
        if all(n in statement for n in needles) and not any(
            a in statement for a in absent
        ):
            return statement
        return None

    return ready


def _status(response) -> int:
    if isinstance(response, requests.Response):
        return response.status_code
    return getattr(response, "status_code", 0)


def test_cross_table_context_sources_follow_source_changes(
    stateful_api, echo_generator
):
    suffix = uuid4().hex[:8]
    orgs = f"ctx_orgs_{suffix}"
    people = f"ctx_people_{suffix}"
    stateful_api.create_table(orgs, num_shards=NUM_SHARDS)
    stateful_api.create_table(people, num_shards=NUM_SHARDS)
    stateful_api.batch_write(
        orgs,
        inserts={
            "org:acme": {"org": "acme", "title": "Acme Alpha"},
            "org:globex": {"org": "globex", "title": "Globex One"},
        },
        sync_level="full_index",
    )

    stateful_api.put(
        f"/tables/{people}/artifacts/brief/enrichment",
        _enrichment(
            echo_generator,
            {
                "peers": {
                    "query": {
                        "table": orgs,
                        "filter_query": {"field": "org", "term": {"$source": "/org"}},
                    },
                    "fields": ["title"],
                    "max_documents": 10,
                }
            },
        ),
    )
    stateful_api.batch_write(
        people,
        inserts={
            "person:1": {"org": "acme", "body": "person one"},
            "person:2": {"org": "globex", "body": "person two"},
        },
        sync_level="enrichments",
    )

    first = wait_until(
        _statement_containing(
            stateful_api, people, "person:1", "brief", "person one", "Acme Alpha"
        ),
        timeout_s=60,
        interval_s=0.25,
    )
    assert first is not None, _statement(stateful_api, people, "person:1", "brief")
    assert "Globex" not in first
    assert not echo_generator.errors, echo_generator.errors

    # A consumed field change on another table and Raft group rebuilds the
    # dependent through notice delivery, inbox apply and leader maintenance.
    stateful_api.batch_write(
        orgs,
        inserts={"org:acme": {"org": "acme", "title": "Acme Beta"}},
        sync_level="full_index",
    )
    updated = wait_until(
        _statement_containing(
            stateful_api,
            people,
            "person:1",
            "brief",
            "Acme Beta",
            absent=("Acme Alpha",),
        ),
        timeout_s=60,
        interval_s=0.25,
    )
    assert updated is not None, _statement(stateful_api, people, "person:1", "brief")

    # Membership: a new matching row joins the dependent's context.
    stateful_api.batch_write(
        orgs,
        inserts={"org:acme-west": {"org": "acme", "title": "Acme West"}},
        sync_level="full_index",
    )
    joined = wait_until(
        _statement_containing(
            stateful_api, people, "person:1", "brief", "Acme Beta", "Acme West"
        ),
        timeout_s=60,
        interval_s=0.25,
    )
    assert joined is not None, _statement(stateful_api, people, "person:1", "brief")

    # Deletion leaves the matching set.
    stateful_api.batch_write(orgs, deletes=["org:acme-west"], sync_level="full_index")
    left = wait_until(
        _statement_containing(
            stateful_api,
            people,
            "person:1",
            "brief",
            "Acme Beta",
            absent=("Acme West",),
        ),
        timeout_s=60,
        interval_s=0.25,
    )
    assert left is not None, _statement(stateful_api, people, "person:1", "brief")

    # An unrelated row does not rebuild person:1; the related dependent does.
    acme_calls = echo_generator.calls_for("person one")
    stateful_api.batch_write(
        orgs,
        inserts={"org:globex": {"org": "globex", "title": "Globex Two"}},
        sync_level="full_index",
    )
    rebuilt = wait_until(
        _statement_containing(stateful_api, people, "person:2", "brief", "Globex Two"),
        timeout_s=60,
        interval_s=0.25,
    )
    assert rebuilt is not None, _statement(stateful_api, people, "person:2", "brief")
    time.sleep(2.0)
    assert echo_generator.calls_for("person one") == acme_calls
    assert not echo_generator.errors, echo_generator.errors


def test_distributed_context_source_admission(stateful_api, echo_generator):
    suffix = uuid4().hex[:8]
    left = f"ctx_left_{suffix}"
    right = f"ctx_right_{suffix}"
    stateful_api.create_table(left, num_shards=NUM_SHARDS)
    stateful_api.create_table(right, num_shards=NUM_SHARDS)

    def peers(table: str, fields: list[str]) -> dict:
        return {
            "peers": {
                "query": {
                    "table": table,
                    "filter_query": {"field": "org", "term": {"$source": "/org"}},
                },
                "fields": fields,
                "max_documents": 10,
            }
        }

    # Unknown source tables are rejected at admission.
    response = stateful_api._request(
        "PUT",
        f"/tables/{left}/artifacts/brief/enrichment",
        _enrichment(echo_generator, peers(f"ctx_missing_{suffix}", ["title"])),
    )
    assert _status(response) == 400, getattr(response, "text", response)

    # A downstream consumer of another table's generated output is admitted;
    # closing the loop back through generated inputs is a cycle.
    stateful_api.put(
        f"/tables/{left}/artifacts/brief/enrichment",
        _enrichment(echo_generator, peers(right, ["title"])),
    )
    stateful_api.put(
        f"/tables/{right}/artifacts/digest/enrichment",
        _enrichment(echo_generator, peers(left, ["_artifacts.brief"])),
    )
    response = stateful_api._request(
        "PUT",
        f"/tables/{left}/artifacts/brief/enrichment",
        _enrichment(echo_generator, peers(right, ["_artifacts.digest"])),
    )
    assert _status(response) == 400, getattr(response, "text", response)
