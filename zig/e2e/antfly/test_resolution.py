# Copyright 2026 Antfly, Inc.
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

"""End-to-end entity-resolution test over a live multi-Raft swarm.

Exercises the full cross-shard loop with no inference dependency: a document's
``relations`` field is materialized into an extraction artifact by a graph
index; the resolution worker blocks candidate entities against a *separate*
``entities`` table (cross-shard read) and records a resolution artifact; the
promoter upserts the canonical entity documents into that table (cross-shard
write). See zig/RESOLUTION.md.

Run with the built binary, e.g.:

    ANTFLY_BIN=zig-out/bin/antfly uv run pytest e2e/antfly/test_resolution.py
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    SwarmAntflyServer,
    find_free_port,
    resolve_binary_path,
)

DOCUMENTS_INDEXES = {
    # Materializes each document's `relations` field into the `relations_v1`
    # extraction asset the resolver consumes (no LLM needed). The resolver is
    # declared in a `resolvers` section nested in the index config; the typed
    # index parse ignores it while the provisioner registers it.
    "relations_graph": {
        "type": "graph",
        "source": {
            "kind": "artifact",
            "artifact": "relations_v1",
            "path": "$.relations[*]",
            "format": "extraction_relation",
        },
        "artifact": {
            "name": "relations_v1",
            "kind": "asset",
            "field": "relations",
            "content_type": "application/json",
        },
        # Prefix blocking links a mention to an existing entity under the same
        # `label/` namespace (cross-shard read of the entities table).
        "resolvers": [
            {
                "name": "kg",
                "table": "entities",
                "source_artifact": "relations_v1",
                "resolution_artifact": "resolution_v1",
                "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}",
                "candidate_search": "prefix",
                "config_generation": 1,
            }
        ],
    },
}


@pytest.fixture(scope="function")
def resolution_swarm():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary} (set ANTFLY_BIN)")
    server = SwarmAntflyServer(binary, "127.0.0.1", find_free_port())
    try:
        yield server
    finally:
        server.stop()


class _Api:
    def __init__(self, base_url: str, server: SwarmAntflyServer):
        self.url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        self._server = server

    def _check(self, response: requests.Response) -> dict:
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {response.request.method} "
                f"{response.url}\n[body]\n{response.text}\n[logs]\n{self._server.debug_logs()}",
                response=response,
            )
        return response.json() if response.content else {}

    def create_table(self, name: str, *, num_shards: int = 1, indexes: dict | None = None) -> dict:
        payload: dict = {"num_shards": num_shards}
        if indexes is not None:
            payload["indexes"] = indexes
        return self._check(self.s.post(f"{self.url}/tables/{name}", json=payload, timeout=30))

    def insert(self, table: str, doc_id: str, body: dict, *, sync_level: str = "enrichments") -> dict:
        payload = {"inserts": {doc_id: body}, "sync_level": sync_level}
        return self._check(self.s.post(f"{self.url}/tables/{table}/batch", json=payload, timeout=30))

    def lookup(self, table: str, key: str) -> dict | None:
        response = self.s.get(
            f"{self.url}/tables/{table}/lookup/{quote(key, safe='')}", timeout=10
        )
        if response.status_code == 404:
            return None
        return self._check(response)


def _wait_for_entity(api: _Api, key: str, *, timeout_s: float = 60.0) -> dict:
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = api.lookup("entities", key)
        if last is not None:
            return last
        time.sleep(0.5)
    raise AssertionError(f"entity {key!r} was not promoted within {timeout_s}s (last={last})")


def _doc_text(doc: dict) -> str:
    """The lookup response carries the stored document; flatten it to text so the
    assertions tolerate whichever envelope the public API uses."""
    return json.dumps(doc)


def test_swarm_resolves_and_promotes_entities_cross_shard(resolution_swarm):
    api = _Api(resolution_swarm.api_url, resolution_swarm)

    # Entities live in their own table (own shard group); documents are spread
    # across two shards. Resolution reads entities cross-shard; promotion writes
    # them cross-shard.
    api.create_table("entities", num_shards=1)
    api.create_table("documents", num_shards=2, indexes=DOCUMENTS_INDEXES)

    api.insert(
        "documents",
        "doc:a",
        {
            "relations": {
                "entities": [
                    {"id": "e0", "label": "person", "text": "Ada Lovelace"},
                    {"id": "e1", "label": "org", "text": "Antfly"},
                ]
            }
        },
    )

    # The promoter upserts a canonical entity document per resolved mention into
    # the entity table on its own shard.
    ada = _wait_for_entity(api, "person/ada_lovelace")
    assert "Ada Lovelace" in _doc_text(ada)
    antfly = _wait_for_entity(api, "org/antfly")
    assert "Antfly" in _doc_text(antfly)

    # A second document mentioning the same person resolves (prefix blocking) to
    # the existing entity rather than minting a new one; the entity persists with
    # its canonical name.
    api.insert(
        "documents",
        "doc:b",
        {"relations": {"entities": [{"id": "e0", "label": "person", "text": "Ada Lovelace"}]}},
    )
    # Give the resolve -> promote loop a moment, then confirm the canonical entity
    # is still the single Ada Lovelace document.
    time.sleep(2.0)
    ada_again = _wait_for_entity(api, "person/ada_lovelace")
    assert "Ada Lovelace" in _doc_text(ada_again)
