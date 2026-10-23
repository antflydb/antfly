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

"""Operator graph ceilings apply to Cypher, SQL HTTP, and pgwire."""

import json
import os
import subprocess
import time
from pathlib import Path

import pytest
import requests
import conftest


@pytest.mark.parametrize(
    "ceiling",
    [
        {"max_scanned_anchors": 1},
        {"max_intermediate_states": 1},
        {"max_retained_state_bytes": 1},
        {"max_explored_edges": 1},
    ],
)
def test_graph_operator_ceilings_cover_sql_and_staged_graphs(monkeypatch, ceiling):
    binary = os.environ.get("ANTFLY_BIN", str(conftest.DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    original_config = conftest._write_remote_content_e2e_config

    def config(root, *, pgwire_port=None):
        path = original_config(root, pgwire_port=pgwire_port)
        values = json.loads(path.read_text())
        values["graph_execution"] = ceiling
        path.write_text(json.dumps(values))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", config)
    server = conftest.StandaloneAntflyServer(
        binary, "127.0.0.1", conftest.find_free_port(), pgwire=True
    )
    client = requests.Session()
    client.auth = ("admin", conftest.AUTH_BOOTSTRAP_PASSWORD)
    base = server.api_url.rstrip("/")

    def sql(statement, session_id=None):
        body = {"statement": statement}
        if session_id:
            body["session_id"] = session_id
        return client.post(base + "/sql", json=body, timeout=30)

    def accepted(response):
        assert response.status_code in (200, 201, 202, 204), response.text
        return response.json()

    statement = "SELECT id FROM GRAPH_TABLE(g MATCH (a IS Document)-[r IS LINK]->(b IS Document) COLUMNS (a.id AS id)) x"
    try:
        accepted(sql("CREATE TABLE docs (id TEXT, ref TEXT)"))
        accepted(
            client.post(
                base + "/tables/docs/indexes/links",
                json={
                    "type": "graph",
                    "edge_types": [{"name": "link", "field": "ref"}],
                },
                timeout=30,
            )
        )
        accepted(
            sql("INSERT INTO docs (_id,id,ref) VALUES ('a','a','b'),('b','b','a')")
        )
        accepted(
            client.post(
                base + "/graphs/g",
                json={
                    "labels": [{"name": "Document", "table": "docs"}],
                    "relationships": [
                        {
                            "name": "LINK",
                            "index": "links",
                            "source_label": "Document",
                            "target_label": "Document",
                            "stored_type": "link",
                        }
                    ],
                },
                timeout=30,
            )
        )
        assert accepted(sql("SELECT id FROM docs ORDER BY id"))["rows"] == [
            ["a"],
            ["b"],
        ]
        direct = client.post(
            base + "/graphs/g/query",
            json={"query": "MATCH (a:Document)-[:LINK]->(b:Document) RETURN a.id"},
            timeout=30,
        )
        assert direct.status_code == 429, direct.text
        response = sql(statement)
        assert response.status_code >= 400, response.text
        assert response.json()["code"] == "54000", response.text
        session_id = accepted(sql("BEGIN"))["session_id"]
        try:
            accepted(sql("UPDATE docs SET id='staged' WHERE _id='a'", session_id))
            response = sql(statement, session_id)
            assert response.status_code >= 400, response.text
            assert response.json()["code"] == "54000", response.text
        finally:
            accepted(sql("ROLLBACK", session_id))
        assert accepted(sql("SELECT id FROM docs ORDER BY id"))["rows"] == [
            ["a"],
            ["b"],
        ]
        psql = Path(os.environ.get("PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql"))
        if psql.exists():
            for query in (
                statement,
                "BEGIN; UPDATE docs SET id='staged' WHERE _id='a'; " + statement,
            ):
                result = subprocess.run(
                    [
                        str(psql),
                        f"host=127.0.0.1 port={server.pgwire_port} user=admin dbname=default sslmode=disable",
                        "-X",
                        "-At",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-v",
                        "VERBOSITY=verbose",
                    ],
                    input=query + ";",
                    text=True,
                    capture_output=True,
                    timeout=30,
                    env={**os.environ, "PGPASSWORD": conftest.AUTH_BOOTSTRAP_PASSWORD},
                )
                assert result.returncode != 0, result.stdout
                assert "54000" in result.stderr, result.stderr
            assert accepted(sql("SELECT id FROM docs ORDER BY id"))["rows"] == [
                ["a"],
                ["b"],
            ]
    finally:
        client.close()
        server.stop()


def test_completed_graph_statements_release_cuts_and_reject_bound_redeclarations():
    binary = os.environ.get("ANTFLY_BIN", str(conftest.DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = conftest.StandaloneAntflyServer(
        binary, "127.0.0.1", conftest.find_free_port(), pgwire=True
    )
    client = requests.Session()
    client.auth = ("admin", conftest.AUTH_BOOTSTRAP_PASSWORD)
    base = server.api_url.rstrip("/")

    def post(path, body, expected=200):
        response = client.post(base + path, json=body, timeout=30)
        assert response.status_code == expected, response.text
        return response.json()

    def query(statement):
        return post("/graphs/g/query", {"query": statement})

    try:
        post("/sql", {"statement": "CREATE TABLE docs (id TEXT)"})
        post("/tables/docs/indexes/links", {"type": "graph"}, expected=201)
        post(
            "/graphs/g",
            {
                "labels": [{"name": "Document", "table": "docs"}],
                "relationships": [
                    {
                        "name": "LINK",
                        "index": "links",
                        "source_label": "Document",
                        "target_label": "Document",
                        "stored_type": "link",
                    }
                ],
            },
            expected=201,
        )
        query("CREATE (:Document {id:'a'})")
        for _ in range(80):
            assert query("MATCH (n:Document) RETURN count(n)")["rows"] == [
                [{"type": "integer", "value": "1"}]
            ]
            assert post(
                "/sql",
                {
                    "statement": "SELECT id FROM GRAPH_TABLE(g MATCH (n IS Document) COLUMNS (n.id AS id)) x"
                },
            )["rows"] == [["a"]]
        # Each failed evaluation also opens a cut; these must be reclaimed.
        for _ in range(70):
            response = client.post(
                base + "/graphs/g/query",
                json={"query": "MATCH (n:Document) RETURN 1/0"},
                timeout=30,
            )
            assert response.status_code == 400, response.text
            assert response.json()["code"] != "QueryCandidateBudgetExceeded"
        for statement in (
            "MATCH (n:Document) CREATE (n:Document {id:'changed'}) RETURN n",
            "MATCH (n:Document) MERGE (n:Document {id:'changed'}) RETURN n",
            "MATCH (n:Document) MERGE (n {id:'changed'})-[:LINK]->(:Document {id:'b'}) ON CREATE SET n.id='changed' RETURN n",
        ):
            rejected = post("/graphs/g/query", {"query": statement}, expected=400)
            assert rejected["code"] == "CypherVariableConflict", rejected
        assert query("MATCH (n:Document) RETURN n.id")["rows"] == [
            [{"type": "string", "value": "a"}]
        ]
        query("MATCH (a:Document {id:'a'}) CREATE (a)-[:LINK]->(:Document {id:'b'})")
        assert query("MATCH (n:Document) RETURN count(n)")["rows"] == [
            [{"type": "integer", "value": "2"}]
        ]
        # The client can receive the response before the handler's deferred
        # final-reader cleanup completes. Allow that cleanup to finish.
        deadline = time.monotonic() + 5
        while list(server.root.rglob("query-cut.json")) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert list(server.root.rglob("query-cut.json")) == []
        assert "Cypher statement cut cleanup failed" not in server.log_path.read_text()
    finally:
        client.close()
        server.stop()
