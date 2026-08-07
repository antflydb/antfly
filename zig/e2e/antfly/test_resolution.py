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

"""End-to-end entity-resolution test over a live multi-node raft cluster.

Exercises the full cross-shard loop with no inference dependency: a document's
``relations`` field is materialized into an extraction artifact by a graph
index; the resolution worker blocks candidate entities against a *separate*
``entities`` table (cross-shard read) and records a resolution artifact; the
promoter upserts the canonical entity documents into that table (cross-shard
write); and the document graph is queried back through the mention edge with
cross-table entity hydration. See zig/RESOLUTION.md.

Run with the built binary, e.g.:

    ANTFLY_BIN=zig-out/bin/antfly uv run pytest e2e/antfly/test_resolution.py
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    resolve_binary_path,
)
from test_scaling import MultiNodeScalingCluster

AUTOGRAPH_E2E_TIMEOUT_S = 115.0
AUTOGRAPH_CLUSTER_STARTUP_TIMEOUT_S = 115.0
AUTOGRAPH_E2E_TEARDOWN_TIMEOUT_S = 5.0
POLL_INTERVAL_S = 0.5
POLL_REQUEST_TIMEOUT_S = 5.0
WRITE_REQUEST_TIMEOUT_FLOOR_S = 5.0


def _new_e2e_deadline() -> "_Deadline":
    return _Deadline(AUTOGRAPH_E2E_TIMEOUT_S)


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
            # Emit a doc->entity provenance edge per mention, tagged with the
            # resolved entity's home table so a graph query starting from a
            # document can hydrate the canonical entity cross-table (DocRef
            # hydration routing).
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "relations_v1",
            "kind": "asset",
            "field": "relations",
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
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
def resolution_cluster():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary} (set ANTFLY_BIN)")
    if Path(binary).name != "antfly":
        pytest.skip("distributed autograph e2e requires the antfly binary")
    startup_deadline = _Deadline(AUTOGRAPH_CLUSTER_STARTUP_TIMEOUT_S)
    cluster = MultiNodeScalingCluster(
        binary,
        initial_data_node_count=3,
        startup_deadline_at=startup_deadline.expires_at,
    )
    try:
        yield cluster
    finally:
        cluster.stop(timeout_s=AUTOGRAPH_E2E_TEARDOWN_TIMEOUT_S)


class _Api:
    def __init__(self, base_url: str, server: MultiNodeScalingCluster):
        urls = [base_url, *server.data_api_urls]
        self._urls = list(dict.fromkeys(url.rstrip("/") for url in urls))
        self._url_index = 0
        self.url = self._urls[self._url_index]
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        self._server = server

    def _advance_url(self) -> None:
        self._url_index = (self._url_index + 1) % len(self._urls)
        self.url = self._urls[self._url_index]

    def _check(self, response: requests.Response) -> dict:
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {response.request.method} "
                f"{response.url}\n[body]\n{response.text}\n[logs]\n{self._server.debug_logs()}",
                response=response,
            )
        return response.json() if response.content else {}

    def create_table(
        self,
        name: str,
        *,
        num_shards: int = 1,
        indexes: dict | None = None,
        deadline: "_Deadline | None" = None,
    ) -> dict:
        payload: dict = {"num_shards": num_shards}
        if indexes is not None:
            payload["indexes"] = indexes
        max_timeout = 90.0 if indexes is not None or num_shards > 1 else 30.0
        timeout = deadline.request_timeout(max_timeout) if deadline is not None else max_timeout
        try:
            response = self.s.post(f"{self.url}/tables/{name}", json=payload, timeout=timeout)
        except requests.RequestException as exc:
            stacks = self._server.native_stack_dumps()
            index_names = sorted(indexes.keys()) if indexes is not None else []
            raise AssertionError(
                f"create table timed out/failed table={name!r} shards={num_shards} "
                f"indexes={index_names!r}: {exc!r}\n[native stacks]\n{stacks}"
                f"\n[logs]\n{self._server.debug_logs()}"
            ) from exc
        return self._check(response)

    def insert(
        self,
        table: str,
        doc_id: str,
        body: dict,
        *,
        sync_level: str = "write",
        deadline: "_Deadline | None" = None,
    ) -> dict:
        payload = {"inserts": {doc_id: body}, "sync_level": sync_level}
        max_timeout = 120.0 if sync_level in {"enrichments", "full_index"} else 30.0
        last_retryable_response: str | None = None
        while True:
            try:
                timeout = (
                    deadline.request_timeout(
                        max_timeout,
                        minimum_timeout_s=WRITE_REQUEST_TIMEOUT_FLOOR_S,
                    )
                    if deadline is not None
                    else max_timeout
                )
            except AssertionError as exc:
                stacks = self._server.native_stack_dumps()
                raise AssertionError(
                    f"batch insert exhausted its {deadline.timeout_s:.1f}s deadline "
                    f"table={table!r} key={doc_id!r} sync_level={sync_level!r} "
                    f"last_retryable_response={last_retryable_response!r}"
                    f"\n[native stacks]\n{stacks}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc
            try:
                response = self.s.post(f"{self.url}/tables/{table}/batch", json=payload, timeout=timeout)
            except requests.RequestException as exc:
                # A timed-out write usually means a node wedged in memory without
                # logging anything; capture native stacks before teardown so the
                # CI failure is diagnosable.
                stacks = self._server.native_stack_dumps()
                raise AssertionError(
                    f"batch insert timed out/failed table={table!r} key={doc_id!r} "
                    f"sync_level={sync_level!r}: {exc!r}\n[native stacks]\n{stacks}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc
            if (
                deadline is not None
                and response.status_code == 503
                and response.text.strip() == "write unavailable"
                and not deadline.expired()
            ):
                last_retryable_response = (
                    f"{response.status_code} {response.text.strip()} from {self.url}"
                )
                # Any data node can route a public write to the shard leader,
                # but one node may temporarily have a stale leader or placement
                # view. Retry through the next live node instead of pinning the
                # entire deadline to that stale routing view.
                self._advance_url()
                deadline.sleep()
                continue
            return self._check(response)

    def lookup(self, table: str, key: str, *, timeout: float = 10.0) -> dict | None:
        response = self.s.get(
            f"{self.url}/tables/{table}/documents/{quote(key, safe='')}", timeout=timeout
        )
        if response.status_code == 404:
            return None
        return self._check(response)

    def query_table(self, table: str, payload: dict, *, timeout: float = 30.0) -> dict:
        return self._check(self.s.post(f"{self.url}/tables/{table}/query", json=payload, timeout=timeout))

    def diagnostic(self, *, graph_payload: dict | None = None) -> str:
        parts: list[str] = []
        for label, path in (
            ("documents table", "/tables/documents"),
            ("relations graph index", "/tables/documents/indexes/relations_graph"),
            ("entities table", "/tables/entities"),
        ):
            try:
                response = self.s.get(f"{self.url}{path}", timeout=5)
                parts.append(f"[{label}] {response.status_code} {response.text[:4000]}")
            except requests.RequestException as exc:
                parts.append(f"[{label}] unavailable: {exc!r}")
        if graph_payload is not None:
            parts.append(self._graph_probe_diagnostic(graph_payload))
        parts.append(f"[metadata snapshot]\n{self._server.metadata_snapshot_diagnostic()}")
        parts.append(f"[logs]\n{self._server.debug_logs()}")
        return "\n".join(parts)

    def _graph_probe_diagnostic(self, payload: dict) -> str:
        parts: list[str] = ["[graph query probes]"]
        for index, base_url in enumerate(self._server.data_api_urls):
            url = base_url.rstrip("/")
            try:
                status = self.s.get(
                    f"{url}/tables/documents/indexes/relations_graph",
                    timeout=5,
                )
                parts.append(
                    f"[data {index} graph index] {status.status_code} {status.text[:3000]}"
                )
            except requests.RequestException as exc:
                parts.append(f"[data {index} graph index] unavailable: {exc!r}")

            try:
                query = self.s.post(
                    f"{url}/tables/documents/query",
                    json=payload,
                    timeout=5,
                )
                parts.append(f"[data {index} graph query] {query.status_code} {query.text[:3000]}")
            except requests.RequestException as exc:
                parts.append(f"[data {index} graph query] unavailable: {exc!r}")
        return "\n".join(parts)


class _Deadline:
    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s
        self.started_at = time.monotonic()
        self.expires_at = time.monotonic() + timeout_s

    def elapsed(self) -> float:
        return max(0.0, time.monotonic() - self.started_at)

    def remaining(self) -> float:
        return max(0.0, self.expires_at - time.monotonic())

    def expired(self) -> bool:
        return self.remaining() <= 0.0

    def request_timeout(
        self,
        max_timeout_s: float = POLL_REQUEST_TIMEOUT_S,
        *,
        minimum_timeout_s: float = 0.1,
    ) -> float:
        remaining = self.remaining()
        if remaining < minimum_timeout_s:
            raise AssertionError(
                f"deadline has {remaining:.3f}s remaining, below the "
                f"{minimum_timeout_s:.3f}s request floor"
            )
        return min(max_timeout_s, remaining)

    def sleep(self) -> None:
        remaining = self.remaining()
        if remaining > 0.0:
            time.sleep(min(POLL_INTERVAL_S, remaining))


def test_insert_fails_over_from_write_unavailable(monkeypatch: pytest.MonkeyPatch):
    class _Server:
        data_api_urls = ["http://data-a/db/v1", "http://data-b/db/v1"]

    api = _Api(_Server.data_api_urls[0], _Server())
    calls: list[str] = []
    responses = [
        (503, b"write unavailable"),
        (201, b""),
    ]

    def post(url: str, **_: Any) -> requests.Response:
        calls.append(url)
        status, body = responses.pop(0)
        response = requests.Response()
        response.status_code = status
        response._content = body
        response.url = url
        return response

    monkeypatch.setattr(api.s, "post", post)
    deadline = _Deadline(10.0)
    monkeypatch.setattr(deadline, "sleep", lambda: None)

    assert api.insert("documents", "doc:b", {}, deadline=deadline) == {}
    assert calls == [
        "http://data-a/db/v1/tables/documents/batch",
        "http://data-b/db/v1/tables/documents/batch",
    ]


def _wait_for_entities(api: _Api, expected_names: dict[str, str], *, deadline: _Deadline) -> dict[str, dict]:
    pending = set(expected_names.keys())
    found: dict[str, dict] = {}
    last: dict[str, dict | None] = {}
    last_error: str | None = None

    while not deadline.expired():
        for key in list(pending):
            try:
                doc = api.lookup("entities", key, timeout=deadline.request_timeout())
            except requests.RequestException as exc:
                if not _transient_poll_error(exc):
                    raise
                last_error = repr(exc)
                continue
            last[key] = doc
            if doc is not None and expected_names[key] in _doc_text(doc):
                found[key] = doc
                pending.remove(key)
        if not pending:
            return found
        deadline.sleep()

    raise AssertionError(
        f"entities were not promoted within {deadline.timeout_s}s "
        f"(elapsed={deadline.elapsed():.1f}s, pending={sorted(pending)!r}, "
        f"last={last!r}, last_error={last_error!r})"
        f"\n[native stacks]\n{api._server.native_stack_dumps()}"
        f"\n{api.diagnostic()}"
    )


def _doc_text(doc: dict) -> str:
    """The lookup response carries the stored document; flatten it to text so the
    assertions tolerate whichever envelope the public API uses."""
    return json.dumps(doc)


def _transient_poll_error(exc: requests.RequestException) -> bool:
    response = getattr(exc, "response", None)
    if response is not None:
        return response.status_code >= 500
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.ReadTimeout,
            requests.Timeout,
        ),
    )


def _graph_result(result: dict, name: str) -> dict | None:
    responses = result.get("responses", [])
    if not responses:
        return None
    return responses[0].get("graph_results", {}).get(name)


def _wait_for_mention_hydration(
    api: _Api,
    *,
    start_node: str,
    expected_names: dict[str, str],
    deadline: _Deadline,
) -> dict:
    payload = {
        "query": {"match_all": {}},
        "graph_searches": {
            "mentions": {
                "type": "neighbors",
                "index_name": "relations_graph",
                "start_nodes": {"keys": [start_node]},
                "params": {
                    "edge_types": ["mentions"],
                    "direction": "out",
                    "max_results": 10,
                },
                "include_documents": True,
                "fields": ["entity_type", "canonical_name", "aliases"],
            }
        },
        "limit": 10,
    }

    last: dict[str, Any] | None = None
    last_error: str | None = None
    while not deadline.expired():
        try:
            last = api.query_table("documents", payload, timeout=deadline.request_timeout())
        except requests.RequestException as exc:
            if not _transient_poll_error(exc):
                raise
            last_error = repr(exc)
            deadline.sleep()
            continue
        graph = _graph_result(last, "mentions")
        if graph is None:
            deadline.sleep()
            continue
        nodes = graph.get("nodes", [])
        by_key = {node.get("key"): node for node in nodes if isinstance(node, dict)}
        hydrated = True
        for key, canonical_name in expected_names.items():
            node = by_key.get(key)
            hydrated = (
                hydrated
                and isinstance(node, dict)
                and isinstance(node.get("document"), dict)
                and node["document"].get("canonical_name") == canonical_name
            )
        if hydrated:
            return graph
        deadline.sleep()
    raise AssertionError(
        f"mention graph did not hydrate promoted entities within {deadline.timeout_s}s "
        f"(elapsed={deadline.elapsed():.1f}s, start_node={start_node!r}, "
        f"expected={expected_names!r}, last={last!r}, last_error={last_error!r})\n"
        f"{api.diagnostic(graph_payload=payload)}"
    )


def test_multinode_autograph_resolves_promotes_and_hydrates_entities(resolution_cluster):
    cluster = resolution_cluster
    api = _Api(cluster.data_api_urls[0], cluster)

    # Entities live in their own table (own shard group); documents are spread
    # across multiple shards in an explicit multi-node metadata/data raft setup.
    # Resolution reads entities cross-shard; promotion writes them cross-shard;
    # graph query hydrates the promoted entity documents through mention edges.
    api.create_table("entities", num_shards=1, deadline=_new_e2e_deadline())
    api.create_table(
        "documents",
        num_shards=3,
        indexes=DOCUMENTS_INDEXES,
        deadline=_new_e2e_deadline(),
    )

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
        deadline=_new_e2e_deadline(),
    )

    # The promoter upserts a canonical entity document per resolved mention into
    # the entity table on its own shard.
    _wait_for_entities(
        api,
        {
            "person/ada_lovelace": "Ada Lovelace",
            "org/antfly": "Antfly",
        },
        deadline=_new_e2e_deadline(),
    )

    # A second document mentioning the same person resolves (prefix blocking) to
    # the existing entity rather than minting a new one; the entity persists with
    # its canonical name.
    api.insert(
        "documents",
        "doc:b",
        {"relations": {"entities": [{"id": "e0", "label": "person", "text": "Ada Lovelace"}]}},
        deadline=_new_e2e_deadline(),
    )

    mentions = _wait_for_mention_hydration(
        api,
        start_node="doc:a",
        expected_names={
            "person/ada_lovelace": "Ada Lovelace",
            "org/antfly": "Antfly",
        },
        deadline=_new_e2e_deadline(),
    )
    assert mentions["type"] == "neighbors"
    node_keys = {node["key"] for node in mentions["nodes"]}
    assert {"person/ada_lovelace", "org/antfly"} <= node_keys

    second_mentions = _wait_for_mention_hydration(
        api,
        start_node="doc:b",
        expected_names={"person/ada_lovelace": "Ada Lovelace"},
        deadline=_new_e2e_deadline(),
    )
    second_node_keys = {node["key"] for node in second_mentions["nodes"]}
    assert "person/ada_lovelace" in second_node_keys
