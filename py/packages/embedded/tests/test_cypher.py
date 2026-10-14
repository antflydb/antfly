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


def test_normalized_create_defaults_nulls_and_label_predicates(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql(
            "CREATE TABLE docs (amount BIGINT DEFAULT 4, total BIGINT GENERATED ALWAYS AS (amount * 3) STORED)"
        )
        for name, predicate in (("all_docs", {}), ("filtered", {"total": 12})):
            database.graph_catalog_json(
                {
                    "action": "put",
                    "name": name,
                    "definition": {"labels": [{"name": "Document", "table": "docs", "where": predicate}]},
                }
            )
        result = database.cypher_json(
            "all_docs", {"query": "CREATE (n:Document {amount:null}) RETURN n.amount,n.total"}
        )
        assert result["rows"] == [[{"type": "null", "value": None}] * 2]
        assert database.sql("SELECT amount,total FROM docs")["rows"] == [[None, None]]
        database.cypher_json("all_docs", {"query": "MATCH (n) DELETE n"})
        database.cypher_json("all_docs", {"query": "CREATE (n:Document) DELETE n"})
        assert database.sql("SELECT amount,total FROM docs")["rows"] == []
        for verb in ("CREATE", "MERGE"):
            result = database.cypher_json("filtered", {"query": f"{verb} (n:Document) RETURN n.amount,n.total"})
            assert [cell["value"] for cell in result["rows"][0]] == ["4", "12"]
            assert database.sql("SELECT amount,total FROM docs")["rows"] == [["4", "12"]]
            database.cypher_json("all_docs", {"query": "MATCH (n) DELETE n"})
        for value in ("null", "7"):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json("filtered", {"query": f"CREATE (n:Document {{amount:{value}}}) RETURN n"})
            assert invalid.value.graph_code == "CypherLabelPredicateViolation"
            assert database.sql("SELECT amount,total FROM docs")["rows"] == []


def test_relationship_postimages_nulls_and_merge_rollback(require_native, aflite_path):
    definition = {
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
    }
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        database.graph_catalog_json({"action": "put", "name": "knowledge", "definition": definition})
        for query in (
            "MERGE (n:Document {name:$key}) RETURN n",
            "UNWIND ['valid',null] AS key MERGE (n:Document {name:key}) RETURN n",
            "MERGE (:Document)-[r:LINK {key:$key}]->(:Document) RETURN r",
        ):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json("knowledge", {"query": query, "parameters": {"key": None}})
            assert invalid.value.graph_code == "CypherInvalidPropertyValue"
            assert database.sql("SELECT count(*) FROM docs")["rows"] == [["0"]]
        result = database.cypher_json(
            "knowledge",
            {
                "query": "CREATE p=(:Document)-[r:LINK {x:1,absent:null,data:{nested:null}}]->(:Document) WITH p,r,[r] AS list,{edge:r} AS map CALL { WITH r SET r.x=2 } SET r.y=r.x RETURN r.x,r.y,relationships(p)[0].x,list[0].x,map.edge.x,keys(r),r.data",
            },
        )
        assert [cell["value"] for cell in result["rows"][0][:5]] == ["2"] * 5
        assert {cell["value"] for cell in result["rows"][0][5]["value"]} == {"x", "y", "data"}
        assert result["rows"][0][6] == {"type": "map", "value": {"nested": {"type": "null", "value": None}}}
    with af.open_hosted(aflite_path) as database:
        row = database.cypher_json("knowledge", {"query": "MATCH ()-[r:LINK]->() RETURN r.x,r.y,keys(r),r.data"})[
            "rows"
        ][0]
        assert [cell["value"] for cell in row[:2]] == ["2", "2"]
        assert {cell["value"] for cell in row[2]["value"]} == {"x", "y", "data"}
        assert row[3] == {"type": "map", "value": {"nested": {"type": "null", "value": None}}}


def test_deleted_entities_reject_reads_and_endpoints_with_durable_rollback(require_native, aflite_path):
    definition = {
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
    }
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, x BIGINT)")
        database.sql("INSERT INTO docs (_id,id,x) VALUES ('a','a',0),('b','b',0)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        database.graph_catalog_json({"action": "put", "name": "knowledge", "definition": definition})
        for statement in (
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) DELETE b CREATE (a)-[:LINK]->(b) RETURN a",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) DELETE a CREATE (a)-[:LINK]->(b) RETURN b",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) DELETE b MERGE (a)-[:LINK]->(b) RETURN a",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CALL { WITH b DELETE b } CREATE (a)-[:LINK]->(b) RETURN a",
            "MATCH (n:Document {id:'a'}) DELETE n RETURN n.x",
            "MATCH (n:Document {id:'a'}) DELETE n RETURN n['x']",
            "MATCH (n:Document {id:'a'}) DELETE n RETURN properties(n)",
            "MATCH (n:Document {id:'a'}) DELETE n RETURN n{.*}",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:LINK {x:42}]->(b) DELETE r SET a.x=r.x RETURN a.x",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE p=(a)-[r:LINK {x:42}]->(b) DETACH DELETE a RETURN relationships(p)[0].x",
        ):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json("knowledge", {"query": statement})
            assert invalid.value.graph_code == "CypherDeletedEntity"
            assert database.sql("SELECT id,x FROM docs ORDER BY id")["rows"] == [["a", "0"], ["b", "0"]]
            with database.open_table("docs") as table:
                assert table.edges("links", "a", "link", 0) == []
        row = database.cypher_json(
            "knowledge",
            {
                "query": "CREATE p=(n:Document {x:1})-[r:LINK {x:2}]->(:Document) DETACH DELETE n RETURN count(n),count(r),elementId(n),elementId(r),n,r,relationships(p)[0]"
            },
        )["rows"][0]
        assert [cell["value"] for cell in row[:2]] == ["1", "1"]
        assert row[2]["type"] == row[3]["type"] == "string"
        assert row[4]["value"]["properties"]["value"] == {}
        assert row[4]["value"]["labels"] == []
        assert row[5]["value"]["properties"]["value"] == {}
        assert row[6]["value"]["properties"]["value"] == {}
    with af.open_hosted(aflite_path) as database:
        assert database.sql("SELECT id,x FROM docs WHERE id IS NOT NULL ORDER BY id")["rows"] == [
            ["a", "0"],
            ["b", "0"],
        ]
        with database.open_table("docs") as table:
            assert table.edges("links", "a", "link", 0) == []
            assert table.lookup("b")["id"] == "b"


def test_document_derived_aliases_resolve_replacements_and_deleted_contents(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, ref TEXT, x BIGINT)")
        with database.open_table("docs") as table:
            table.add_index(
                {"name": "links", "kind": "graph", "config_json": '{"edge_types":[{"name":"link","field":"ref"}]}'}
            )
        database.sql("INSERT INTO docs (_id,id,ref,x) VALUES ('a','a','b',0),('b','b',null,0)")
        database.graph_catalog_json(
            {
                "action": "put",
                "name": "knowledge",
                "definition": {
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
            }
        )
        row = database.cypher_json(
            "knowledge", {"query": "MATCH (a:Document {id:'a'})-[r:LINK]->(b) SET a.x=2 RETURN r.weight"}
        )["rows"][0]
        assert row == [{"type": "number", "value": 1}]
        for expression in ("r.weight", "r{.*}", "properties(r)"):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json(
                    "knowledge",
                    {"query": f"MATCH (a:Document {{id:'a'}})-[r:LINK]->(b) SET a.ref=null RETURN {expression}"},
                )
            assert invalid.value.graph_code == "CypherDeletedEntity"
            assert database.sql("SELECT ref,x FROM docs WHERE _id='a'")["rows"] == [["b", "2"]]
