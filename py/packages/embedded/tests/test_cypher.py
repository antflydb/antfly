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

import uuid
from contextlib import closing

import pytest

import antfly_embedded as af


def test_graph_catalog_writes_and_receipts_survive_reopen(require_native, aflite_path):
    definition = {
        "labels": [{"name": "Document", "table": "docs"}],
        "relationships": [
            {
                "name": "KNOWS",
                "index": "links",
                "source_label": "Document",
                "target_label": "Document",
                "stored_type": "knows",
            }
        ],
    }
    write_id = uuid.uuid4().hex
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, title TEXT)")
        database.sql("INSERT INTO docs (_id,id,title) VALUES ('a','a','Alpha'),('b','b','Beta')")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        catalog = database.graph_catalog_json({"action": "put", "name": "knowledge", "definition": definition})
        assert catalog["revision"] == "1"
        parameters = database.cypher_json(
            "knowledge",
            {
                "query": "RETURN $min,$max",
                "typed_parameters": {
                    "type": "map",
                    "value": {
                        "min": {"type": "integer", "value": str(-(2**63))},
                        "max": {"type": "integer", "value": str(2**63 - 1)},
                    },
                },
            },
        )
        assert [cell["value"] for cell in parameters["rows"][0]] == [str(-(2**63)), str(2**63 - 1)]
        write = {
            "query": "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:KNOWS {since:2020}]->(b) RETURN r.since",
            "write_request_id": write_id,
        }
        result = database.cypher_json("knowledge", write)
        assert result["commit"]["transaction_id"] == write_id
        assert result["commit"]["status"] == "committed"
        with pytest.raises(af.GraphError) as duplicate:
            database.cypher_json("knowledge", write)
        assert duplicate.value.graph_code == "CypherWriteRequestAlreadyUsed"
        assert duplicate.value.diagnostic["write_outcome"] == "unknown"
        with pytest.raises(af.GraphError):
            database.cypher_json("knowledge", {"query": "CREATE (n:Document {id:'aborted'}) RETURN 1/0"})
        assert database.cypher_json("knowledge", {"query": "MATCH (n:Document {id:'aborted'}) RETURN n"})["rows"] == []
    with af.open_hosted(aflite_path) as database:
        rows = database.cypher_json("knowledge", {"query": "MATCH (a)-[r:KNOWS]->(b) RETURN a.id,r.since,b.id"})["rows"]
        assert [[value["value"] for value in row] for row in rows] == [["a", "2020", "b"]]
        receipt = database.graph_write_status(write_id)
        assert receipt["disposition"] == "committed" and receipt["durable"]
        assert "tables" not in receipt and "rows" not in receipt
        with pytest.raises(af.GraphError):
            database.graph_catalog_json(
                {"action": "put", "name": "knowledge", "expected_revision": "0", "definition": definition}
            )
        replacement = database.graph_catalog_json(
            {"action": "put", "name": "knowledge", "expected_revision": "1", "definition": definition}
        )
        assert replacement["incarnation"] == catalog["incarnation"] and replacement["revision"] == "2"
        database.drop_table("docs")
        database.sql("CREATE TABLE docs (id TEXT, title TEXT)")
        with pytest.raises(af.GraphError) as stale:
            database.cypher_json("knowledge", {"query": "MATCH (n) RETURN n"})
        assert stale.value.graph_code == "CatalogGenerationChanged"
        assert database.graph_write_status(write_id)["disposition"] == "committed"


def test_graph_table_joins_and_staged_sql_postimages(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, title TEXT)")
        database.sql("INSERT INTO docs (_id,id,title) VALUES ('a','a','Alpha'),('b','b','Beta')")
        database.graph_catalog_json(
            {"action": "put", "name": "knowledge", "definition": {"labels": [{"name": "Document", "table": "docs"}]}}
        )
        statement = "SELECT g.id,d.title FROM GRAPH_TABLE(knowledge MATCH (n IS Document) COLUMNS (n.id AS id)) g JOIN docs d ON d.id=g.id ORDER BY g.id"
        assert database.sql(statement)["rows"] == [["a", "Alpha"], ["b", "Beta"]]
        with closing(database.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("UPDATE docs SET title='Changed' WHERE id='a'")
            session.execute("INSERT INTO docs (_id,id,title) VALUES ('c','c','Gamma')")
            session.execute("DELETE FROM docs WHERE id='b'")
            assert session.execute(statement)["rows"] == [["a", "Changed"], ["c", "Gamma"]]
            session.execute("ROLLBACK")
        assert database.sql(statement)["rows"] == [["a", "Alpha"], ["b", "Beta"]]
        parameterized = (
            "SELECT g.id FROM GRAPH_TABLE(knowledge MATCH (n IS Document) WHERE n.id=$1 COLUMNS (n.id AS id)) g"
        )
        assert database.sql(parameterized, ["a"])["rows"] == [["a"]]
