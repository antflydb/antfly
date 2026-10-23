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

"""Native Cypher semantic/hybrid ranking through an actual provider transport."""

import uuid

import pytest


@pytest.mark.parametrize("num_shards", [1, 2])
def test_cypher_semantic_and_hybrid_owner_cut_search(
    backup_api, openai_embedder, num_shards
):
    table = "cypher_semantic_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_graph = False
    backup_api.create_table(table, num_shards=num_shards)
    try:
        backup_api.create_index(
            table,
            "semantic_idx",
            {
                "name": "semantic_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            },
        )
        backup_api.wait_index_ready(
            table, "semantic_idx", timeout_s=30, until="complete"
        )
        backup_api.batch_write(
            table,
            inserts={
                "a": {"id": "a", "body": "alpha concept overview"},
                "b": {"id": "b", "body": "beta concept overview"},
            },
            sync_level="full_index",
        )
        backup_api.post(
            f"/graphs/{graph}",
            {"labels": [{"name": "Document", "table": table}], "relationships": []},
        )
        created_graph = True
        for mode in ("semantic", "hybrid"):
            result = backup_api.post(
                f"/graphs/{graph}/query",
                {
                    "query": f"CALL antfly.search('{table}', {{query:$query,mode:'{mode}',indexes:['semantic_idx'],limit:2}}) YIELD node,score RETURN node.id,score",
                    "parameters": {"query": "beta concept"},
                },
            )
            assert result["complete"] is True, result
            assert result["rows"][0][0] == {"type": "string", "value": "b"}, result
            assert result["rows"][0][1]["type"] in ("integer", "number"), result
            assert result["stats"]["search_calls"] == 1, result
            assert result["stats"]["search_candidates"] >= 2, result
    finally:
        if created_graph:
            backup_api.delete(f"/graphs/{graph}")
        backup_api.delete_table(table)


@pytest.mark.parametrize("num_shards", [1, 2])
@pytest.mark.parametrize("predicate_label", [False, True])
def test_authenticated_cypher_protected_semantic_and_hybrid(
    openai_embedder, monkeypatch, num_shards, predicate_label
):
    import os
    import time
    from pathlib import Path
    import requests
    from conftest import (
        AUTH_BOOTSTRAP_PASSWORD,
        DEFAULT_ANTFLY_BIN,
        StandaloneAntflyServer,
        find_free_port,
    )

    monkeypatch.setenv(
        "ANTFLY_TRUSTED_PRINCIPAL_SECRET", "cypher-semantic-policy-secret-32-bytes"
    )
    monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_ISSUER", "cypher-semantic-policy")
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    session = requests.Session()
    session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    session.headers["Content-Type"] = "application/json"
    base = server.api_url.rstrip("/")
    table = "cypher_semantic_policy_" + uuid.uuid4().hex
    graph = table + "_graph"
    try:

        def call(path, body=None, method="POST", accepted=(200, 201, 202, 204)):
            response = session.request(method, base + path, json=body, timeout=40)
            assert response.status_code in accepted, (
                path,
                response.status_code,
                response.text,
                server.log_path.read_text()[-6000:],
            )
            return response.json() if response.content else None

        schema = {
            "storage_mode": "relational",
            "default_type": "row",
            "column_defaults": [],
            "generated_columns": [],
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            name: {
                                "type": "keyword",
                                "nullable": True,
                                "x-antfly-sql-type": "text",
                            }
                            for name in ("id", "body", "tenant")
                        },
                        "required": [],
                        "additionalProperties": False,
                    }
                }
            },
        }
        call(f"/tables/{table}", {"num_shards": num_shards, "schema": schema})
        call(
            f"/tables/{table}/indexes/semantic_idx",
            {
                "name": "semantic_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            },
        )
        call(
            f"/tables/{table}/batch",
            {
                "sync_level": "full_index",
                "inserts": {
                    "a": {
                        "id": "a",
                        "body": "alpha concept overview",
                        "tenant": "visible",
                    },
                    "b": {
                        "id": "b",
                        "body": "beta concept overview",
                        "tenant": "visible",
                    },
                    "h": {"id": "h", "body": "beta secret concept", "tenant": "hidden"},
                },
            },
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [
                    {
                        "name": "Document",
                        "table": table,
                        **({"where": {"tenant": "visible"}} if predicate_label else {}),
                    }
                ],
                "relationships": [],
            },
        )
        call(
            "/sql",
            {
                "statement": f"CREATE POLICY visible ON {table} USING (tenant='visible') WITH CHECK (tenant='visible')"
            },
        )
        call("/sql", {"statement": f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"})
        deadline = time.monotonic() + 30
        while True:
            visible = call(
                f"/graphs/{graph}/query",
                {"query": "MATCH (n:Document) RETURN n.id ORDER BY n.id"},
                accepted=(200, 403, 409, 503),
            )
            if visible.get("rows") == [
                [{"type": "string", "value": "a"}],
                [{"type": "string", "value": "b"}],
            ]:
                break
            assert time.monotonic() < deadline, visible
            time.sleep(0.1)
        for mode in ("semantic", "hybrid"):
            result = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"CALL antfly.search('{table}', {{query:'beta concept',mode:'{mode}',indexes:['semantic_idx'],limit:10}}) YIELD node,score RETURN node.id,score"
                },
            )
            assert result["complete"] is True, result
            assert result["rows"][0][0] == {"type": "string", "value": "b"}, result
            assert {row[0]["value"] for row in result["rows"]} == {"a", "b"}, result
        created = call(
            f"/graphs/{graph}/query",
            {
                "query": "CREATE (n:Document {id:'c',body:'beta new concept',tenant:'visible'}) RETURN n.id"
            },
        )
        assert created["complete"] is True and created["commit"]["status"] in (
            "committed",
            "committed_pending",
            "committed_repair_required",
        ), created
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": f"CALL antfly.search('{table}', {{query:'beta concept',mode:'semantic',indexes:['semantic_idx'],limit:10}}) YIELD node RETURN node.id"
            },
        )
        assert {row[0]["value"] for row in result["rows"]} == {"a", "b", "c"}, result
    finally:
        session.close()
        server.stop()


def test_authenticated_cypher_hides_relationship_from_hidden_producer(monkeypatch):
    import os
    import time
    from pathlib import Path
    import requests
    from conftest import (
        AUTH_BOOTSTRAP_PASSWORD,
        DEFAULT_ANTFLY_BIN,
        StandaloneAntflyServer,
        find_free_port,
    )

    monkeypatch.setenv(
        "ANTFLY_TRUSTED_PRINCIPAL_SECRET", "cypher-producer-policy-secret-32-bytes"
    )
    monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_ISSUER", "cypher-producer-policy")
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    session = requests.Session()
    session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    base = server.api_url.rstrip("/")
    table = "cypher_producer_" + uuid.uuid4().hex
    graph = table + "_graph"
    try:

        def call(path, body=None, accepted=(200, 201, 202, 204)):
            response = session.post(base + path, json=body, timeout=40)
            assert response.status_code in accepted, (
                path,
                response.status_code,
                response.text,
                server.log_path.read_text()[-8000:],
            )
            return response.json() if response.content else None

        schema = {
            "storage_mode": "relational",
            "default_type": "row",
            "column_defaults": [],
            "generated_columns": [],
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            name: {
                                "type": "keyword",
                                "nullable": True,
                                "x-antfly-sql-type": "text",
                            }
                            for name in ("id", "body", "tenant")
                        },
                        "required": [],
                        "additionalProperties": False,
                    }
                }
            },
        }
        call(f"/tables/{table}", {"schema": schema})
        call(
            f"/tables/{table}/indexes/links",
            {"type": "graph", "edge_types": [{"name": "cites"}]},
        )
        call(
            f"/tables/{table}/batch",
            {
                "sync_level": "full_index",
                "inserts": {
                    "a": {"id": "a", "tenant": "visible"},
                    "b": {"id": "b", "tenant": "visible"},
                    "h": {"id": "h", "tenant": "hidden"},
                },
                "graph_writes": [
                    {
                        "index_name": "links",
                        "source": "a",
                        "target": "b",
                        "edge_type": "cites",
                        "edge_id": "hidden-fact",
                        "owner_document": "h",
                        "metadata_json": '{"note":"secret edge"}',
                    }
                ],
            },
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [{"name": "Document", "table": table}],
                "relationships": [
                    {
                        "name": "CITES",
                        "index": "links",
                        "source_label": "Document",
                        "target_label": "Document",
                        "stored_type": "cites",
                    }
                ],
            },
        )
        query = {
            "query": "MATCH (:Document {id:'a'})-[r:CITES]->(:Document {id:'b'}) RETURN r.note"
        }
        assert call(f"/graphs/{graph}/query", query)["rows"] == [
            [{"type": "string", "value": "secret edge"}]
        ]
        call(
            "/sql",
            {
                "statement": f"CREATE POLICY visible ON {table} USING (tenant='visible') WITH CHECK (tenant='visible')"
            },
        )
        call("/sql", {"statement": f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"})
        deadline = time.monotonic() + 30
        while True:
            result = call(
                f"/graphs/{graph}/query",
                {"query": "MATCH (n:Document) RETURN n.id ORDER BY n.id"},
                accepted=(200, 403, 409, 503),
            )
            if result.get("rows") == [
                [{"type": "string", "value": value}] for value in ("a", "b")
            ]:
                break
            assert time.monotonic() < deadline, result
            time.sleep(0.1)
        assert call(f"/graphs/{graph}/query", query)["rows"] == []
    finally:
        session.close()
        server.stop()


def test_predicate_label_search_does_not_enumerate_or_cap_the_table(backup_api):
    table = "cypher_large_search_" + uuid.uuid4().hex
    graph = table + "_graph"
    backup_api.create_table(table, num_shards=1)
    created_graph = False
    try:
        # A selective posting beyond the old 65,536-row membership capture.
        # Nonmembers still occupy the table; only the posting's parent needs
        # graph membership evaluation before native top-k.
        count = 66_000
        for start in range(0, count, 2000):
            backup_api.batch_write(
                table,
                inserts={
                    f"n{i:06d}": {
                        "id": str(i),
                        "body": "needle" if i == count - 1 else "haystack",
                        "tenant": "visible" if i == count - 1 else "hidden",
                    }
                    for i in range(start, min(start + 2000, count))
                },
                sync_level="full_index",
            )
        backup_api.post(
            f"/graphs/{graph}",
            {
                "labels": [
                    {"name": "Document", "table": table, "where": {"tenant": "visible"}}
                ],
                "relationships": [],
            },
        )
        created_graph = True
        result = backup_api.post(
            f"/graphs/{graph}/query",
            {
                "query": f"CALL antfly.search('{table}', {{query:'body:needle',mode:'text',limit:1}}) YIELD node RETURN node.id",
                "limits": {"scanned_anchors": 4},
            },
        )
        assert result["complete"] is True, result
        assert result["rows"] == [[{"type": "string", "value": str(count - 1)}]], result
        assert result["stats"]["scanned_anchors"] <= 4, result
    finally:
        if created_graph:
            backup_api.delete(f"/graphs/{graph}")
        backup_api.delete_table(table)
