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

"""Operator graph ceilings apply to Cypher, SQL HTTP, and pgwire."""

import json
import os
import subprocess
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
