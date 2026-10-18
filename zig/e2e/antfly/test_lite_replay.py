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
import math
import os
import random

import pytest
import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    OpenAiEmbeddingServer,
    StandaloneAntflyServer,
    resolve_binary_path,
    ready_index_status,
)
from helpers import wait_until


class DenseEmbeddingServer(OpenAiEmbeddingServer):
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
    embedder = DenseEmbeddingServer()
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

        call("POST", "/tables/docs", {})
        call(
            "POST",
            "/tables/docs/indexes/vectors",
            {
                "type": "embeddings",
                "field": "body",
                "dimension": 1024,
                "distance_metric": "cosine",
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": embedder.url + "/v1",
                    "api_key": "stub",
                },
            },
        )

        def write(start, stop):
            call(
                "POST",
                "/tables/docs/batch",
                {
                    "inserts": {
                        f"doc-{i:05d}": {
                            "body": f"document {i} " + "orchard harvest ledger " * 100
                        }
                        for i in range(start, stop)
                    },
                    "sync_level": "write",
                },
            )

        def assert_complete(count):
            status = wait_until(
                lambda: call("GET", "/tables/docs/indexes/vectors"),
                timeout_s=60,
                ready_when=lambda value: bool(
                    value
                    and ready_index_status(value, until="complete")
                    and value["status"].get("total_indexed") == count
                ),
            )
            assert status is not None, (
                call("GET", "/tables/docs/indexes/vectors"),
                server.debug_logs(),
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

        for offset in range(0, 608, batch_size):
            write(offset, min(offset + batch_size, 608))
        assert_complete(608)
        # Restart with fresh debt instead of checking only already settled data.
        write(608, 658)
        server.restart()
        assert_complete(658)
        assert "untransportable error" not in server.debug_logs()
        failed = False
    finally:
        if server is not None:
            server.stop(test_failed=failed)
        embedder.stop()
