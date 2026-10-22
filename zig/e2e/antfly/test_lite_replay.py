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

"""Bulk replay must retain consumer stop signals across compiled owners (#1185)."""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import time

import pytest
import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    InferenceEmbeddingServer,
    StandaloneAntflyServer,
    ready_index_status,
    resolve_binary_path,
)
from helpers import wait_until

# These tests construct their servers directly, outside the process-owning
# fixtures that the scheduler can infer. Reserve their actual process capacity.
pytestmark = pytest.mark.e2e_resource("antfly_process")


class DenseEmbeddingServer(InferenceEmbeddingServer):
    @staticmethod
    def _vector_for_text(text: str) -> list[float]:
        # Deliberately large generated artifacts force bounded replay windows.
        rng = random.Random(hashlib.sha256(text.encode()).digest())
        vector = [rng.gauss(0, 1) for _ in range(1024)]
        norm = math.sqrt(sum(value * value for value in vector))
        return [value / norm for value in vector]


@pytest.mark.parametrize("batch_size", [50, 608])
@pytest.mark.parametrize("storage_engine", [None, "lite"], ids=["default", "lite"])
def test_bulk_dense_replay_and_reopen(batch_size, storage_engine):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    identity = "0123456789abcdef" * 4
    embedder = DenseEmbeddingServer(model_identity=identity, response_delay_s=None)
    server = None
    failed = True
    try:
        server = StandaloneAntflyServer(
            binary, "127.0.0.1", 0, storage_engine=storage_engine
        )

        def call(method, path, payload=None):
            response = requests.request(
                method, server.api_url + path, json=payload, timeout=60
            )
            assert response.ok, (
                response.status_code,
                response.text,
                server.debug_logs(),
            )
            return response.json()

        call("POST", "/tables/concepts", {})
        vectors = {
            "type": "embeddings",
            "field": "body",
            "dimension": 1024,
            "distance_metric": "cosine",
            "embedder": {
                "provider": "antfly",
                "model": "google/embeddinggemma-2",
                "api_url": embedder.url,
                "model_identity": identity,
            },
        }
        call(
            "POST",
            "/tables/docs",
            {"indexes": {"vectors": vectors}},
        )
        created = call(
            "POST",
            "/tables/docs/indexes/knowledge",
            {
                "type": "graph",
                "source": {
                    "artifact": "extraction_v1",
                    "format": "extraction_relation",
                    "path": "$.relations[*]",
                    "mention_edge_type": "mentions",
                },
                "artifact": {
                    "name": "extraction_v1",
                    "kind": "asset",
                    "source": {"type": "field", "value": "extraction"},
                    "content_type": "application/json",
                },
                "edge_types": [{"name": "mentions"}, {"name": "related"}],
                "resolvers": [
                    {
                        "name": "concepts",
                        "table": "concepts",
                        "source_artifact": "extraction_v1",
                        "resolution_artifact": "resolved",
                        "key_template": "concept/{{uslug _entity.text}}",
                    }
                ],
            },
        )
        assert created
        assert (
            call("GET", "/tables/docs/indexes/vectors")["config"]["embedder"][
                "model_identity"
            ]
            == identity
        )

        def write(start, stop):
            call(
                "POST",
                "/tables/docs/batch",
                {
                    "inserts": {
                        f"doc-{i:05d}": {
                            "body": f"document {i} " + "orchard harvest ledger " * 100,
                            "extraction": {
                                "entities": [
                                    {
                                        "id": "e0",
                                        "label": "concept",
                                        "text": f"Orchard {i}",
                                    },
                                    {"id": "e1", "label": "concept", "text": "Harvest"},
                                ],
                                "relations": [
                                    {"source": "e0", "target": "e1", "type": "related"}
                                ],
                            },
                        }
                        for i in range(start, stop)
                    },
                    "sync_level": "write",
                },
            )

        def assert_complete(count):
            last_progress = (0,) * 8
            last_progress_at = time.monotonic()

            def diagnostic(value):
                return (
                    f"Expected {count} indexed vectors:\n"
                    + json.dumps(value, indent=2)
                    + "\n"
                    + server.debug_logs()
                )

            def complete(value):
                nonlocal last_progress, last_progress_at
                state = value.get("status", {})
                worker = state.get("enrichment_runtime", {})
                if worker.get("worker_failed"):
                    raise AssertionError(diagnostic(value))
                # Source-receipt migration can keep replay advancing after
                # every vector is present. Follow that durable work as well
                # as embedding throughput while awaiting final publication.
                progress = (
                    state.get("total_indexed", 0) or 0,
                    worker.get("embed_items_completed", 0) or 0,
                    worker.get("applied_sequence", 0) or 0,
                    worker.get("processed_requests", 0) or 0,
                    worker.get("skip_by_hash_count", 0) or 0,
                    state.get("replay_applied_sequence", 0) or 0,
                    state.get("replay_target_sequence", 0) or 0,
                    state.get("catch_up_applied_sequence", 0) or 0,
                )
                now = time.monotonic()
                if any(
                    current > previous
                    for current, previous in zip(progress, last_progress)
                ):
                    last_progress_at = now
                last_progress = tuple(
                    max(current, previous)
                    for current, previous in zip(progress, last_progress)
                )
                if now - last_progress_at >= 60:
                    raise AssertionError(
                        "No derived replay progress for 60 seconds.\n"
                        + diagnostic(value)
                    )
                return bool(
                    ready_index_status(value, until="complete")
                    and state.get("total_indexed") == count
                )

            # Combined graph resolution and native Lite publication perform
            # substantially more durable work than the vector-only workload.
            # Bound total runtime while detecting worker death or lost progress
            # independently of throughput on Debug builds and shared runners.
            status = wait_until(
                lambda: call("GET", "/tables/docs/indexes/vectors"),
                timeout_s=300,
                ready_when=complete,
            )
            if status is None:
                raise AssertionError(
                    diagnostic(call("GET", "/tables/docs/indexes/vectors"))
                )
            graph_progress = ()
            graph_progress_at = time.monotonic()

            def graph_complete(value):
                nonlocal graph_progress, graph_progress_at
                state = value.get("status", {})
                progress = tuple(
                    state.get(key, 0)
                    for key in (
                        "replay_applied_sequence",
                        "replay_target_sequence",
                        "total_edges",
                        "node_count",
                    )
                )
                now = time.monotonic()
                if not graph_progress or any(
                    current > previous
                    for current, previous in zip(progress, graph_progress)
                ):
                    graph_progress_at = now
                graph_progress = tuple(
                    max(current, previous)
                    for current, previous in zip(progress, graph_progress or progress)
                )
                if (
                    state.get("backfill_state") == "failed"
                    or state.get("readiness", {}).get("state") == "failed"
                    or now - graph_progress_at >= 60
                ):
                    raise AssertionError(
                        "Graph recovery failed or made no progress for 60 seconds.\n"
                        + json.dumps(value, indent=2)
                        + "\n"
                        + server.debug_logs()
                    )
                return ready_index_status(value, until="complete")

            assert wait_until(
                lambda: call("GET", "/tables/docs/resolvers/concepts"),
                timeout_s=60,
                ready_when=lambda value: (
                    value["progress"]["applied"]
                    and bool(value["progress"]["shards"])
                    and all(
                        row["available"] and row["fresh"]
                        for row in value["progress"]["shards"]
                    )
                ),
            ), server.debug_logs()
            # Resolver/promotion commits can publish new graph inputs. Observe
            # their completion before fencing the downstream graph generation.
            assert wait_until(
                lambda: call("GET", "/tables/docs/indexes/knowledge"),
                timeout_s=300,
                ready_when=graph_complete,
            ), (
                json.dumps(call("GET", "/tables/docs/indexes/knowledge"), indent=2)
                + "\n"
                + server.debug_logs()
            )
            result = call(
                "POST",
                "/tables/docs/query",
                {
                    "semantic_search": "orchard",
                    "indexes": ["vectors"],
                    "limit": count + 1,
                    "search_effort": 1.0,
                },
            )
            hits = result["responses"][0]["hits"]["hits"]
            assert {hit["_id"] for hit in hits} == {
                f"doc-{i:05d}" for i in range(count)
            }
            for ordinal in (0, count - 1):
                graph = call(
                    "POST",
                    "/tables/docs/query",
                    {
                        "query": {"match_all": {}},
                        "graph_queries": {
                            "neighbors": {
                                "index": "knowledge",
                                "traverse": {
                                    "start": {"keys": [f"doc-{ordinal:05d}"]},
                                    "edge_types": ["mentions"],
                                    "max_depth": 1,
                                    "include_documents": True,
                                    "limit": 10,
                                },
                            }
                        },
                        "limit": 1,
                    },
                )["responses"][0]["graph_results"]["neighbors"]
                assert {
                    node.get("document", {}).get("canonical_name")
                    for node in graph["nodes"]
                    if node.get("table") == "concepts"
                } == {f"Orchard {ordinal}", "Harvest"}, {
                    "graph": graph,
                    "index": call("GET", "/tables/docs/indexes/knowledge"),
                    "resolver": call("GET", "/tables/docs/resolvers/concepts"),
                    "logs": server.debug_logs(),
                }

        for offset in range(0, 608, batch_size):
            write(offset, min(offset + batch_size, 608))
        assert_complete(608)
        # Hold the producer so reopening necessarily encounters durable debt.
        embedder.arm_delay()
        try:
            write(608, 658)
            assert (
                call("GET", "/tables/docs/indexes/vectors")["status"]["total_indexed"]
                < 658
            )
            server.pause()
        finally:
            embedder.release_delay()
        server.resume()
        assert_complete(658)
        assert embedder.received_model_identities
        assert set(embedder.received_model_identities) == {identity}
        # Neither a changed nor a missing remote identity may yield vectors in
        # the pinned index's space after its catalog was reopened.
        for invalid in ("f" * 64, None):
            embedder.model_identity = invalid
            try:
                response = requests.post(
                    server.api_url + "/tables/docs/query",
                    json={
                        "semantic_search": f"uncached checkpoint mismatch probe {invalid}",
                        "indexes": ["vectors"],
                        "limit": 1,
                    },
                    timeout=60,
                )
                assert not response.ok, response.text
            finally:
                embedder.model_identity = identity
        call(
            "POST",
            "/tables/docs/query",
            {
                "semantic_search": "uncached restored checkpoint probe",
                "indexes": ["vectors"],
                "limit": 1,
            },
        )
        assert "untransportable error" not in server.debug_logs()
        failed = False
    finally:
        if server is not None:
            server.stop(test_failed=failed)
        embedder.stop()


@pytest.mark.parametrize("storage_engine", [None, "lite"], ids=["default", "lite"])
def test_embedding_identity_failures_are_isolated_without_retry(storage_engine):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    identity = "0123456789abcdef" * 4
    embedder = InferenceEmbeddingServer(model_identity=identity)
    server = None
    failed = True
    try:
        server = StandaloneAntflyServer(
            binary, "127.0.0.1", 0, storage_engine=storage_engine
        )

        def call(method, path, payload=None):
            response = requests.request(
                method, server.api_url + path, json=payload, timeout=30
            )
            assert response.ok, (
                response.status_code,
                response.text,
                server.debug_logs(),
            )
            return response.json()

        call(
            "POST",
            "/tables/docs",
            {
                "indexes": {
                    "vectors": {
                        "type": "embeddings",
                        "field": "body",
                        "dimension": 3,
                        "embedder": {
                            "provider": "antfly",
                            "model": "google/embeddinggemma-2",
                            "api_url": embedder.url,
                            "model_identity": identity,
                        },
                    }
                }
            },
        )
        for ordinal, invalid in enumerate((None, "f" * 64, 7)):
            calls_before = len(embedder.received_corpus_model_identities)
            embedder.model_identity = invalid
            call(
                "POST",
                "/tables/docs/batch",
                {
                    "inserts": {
                        f"bad-{ordinal}": {"body": f"invalid identity {ordinal}"}
                    },
                    "sync_level": "write",
                },
            )
            status = wait_until(
                lambda: call("GET", "/tables/docs/indexes/vectors"),
                timeout_s=15,
                ready_when=lambda value, expected=ordinal + 1: (
                    value.get("status", {})
                    .get("coverage", {})
                    .get("terminal_failed", 0)
                    == expected
                ),
            )
            assert status, server.debug_logs()
            assert not status["status"]["enrichment_runtime"]["worker_failed"], status
            assert len(embedder.received_corpus_model_identities) == calls_before + 1, (
                server.debug_logs()
            )

        # A later valid document must still run through the same worker. The
        # rejected sources retain their explicit terminal coverage outcomes.
        embedder.model_identity = identity
        call(
            "POST",
            "/tables/docs/batch",
            {
                "inserts": {"good": {"body": "valid identity"}},
                "sync_level": "write",
            },
        )
        status = wait_until(
            lambda: call("GET", "/tables/docs/indexes/vectors"),
            timeout_s=30,
            ready_when=lambda value: value.get("status", {}).get("total_indexed") == 1,
        )
        assert status, server.debug_logs()
        assert status["status"]["coverage"]["terminal_failed"] == 3, status
        assert not status["status"]["enrichment_runtime"]["worker_failed"], status
        assert "untransportable error" not in server.debug_logs()
        failed = False
    finally:
        if server is not None:
            server.stop(test_failed=failed)
        embedder.stop()
