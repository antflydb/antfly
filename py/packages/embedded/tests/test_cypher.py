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

import json
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
        database.sql("CREATE TABLE docs (id TEXT, title TEXT DEFAULT 'default')")
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
            session.execute("INSERT INTO docs (_id,id,title) VALUES ('d','d',NULL)")
            assert session.execute(
                "SELECT g.title FROM GRAPH_TABLE(knowledge MATCH (n IS Document) COLUMNS (n.id AS id, n.title AS title)) g WHERE g.id='d'"
            )["rows"] == [[None]]
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

        for observation in (
            "WITH a,b,r,count(r) AS seen",
            "WITH a,b,r,elementId(r) AS seen",
            "WITH a,b,r,r AS missing",
        ):
            row = database.cypher_json(
                "knowledge",
                {
                    "query": "MATCH (a:Document {id:'a'})-[r:LINK]->(b) SET a.ref=null "
                    + observation
                    + " SET a.ref=antfly.documentKey(b) WITH a,b,r MATCH (a)-[s:LINK]->(b) RETURN r.weight,s.weight"
                },
            )["rows"][0]
            assert row == [{"type": "number", "value": 1}] * 2
        row = database.cypher_json(
            "knowledge",
            {
                "query": "MATCH p=(a:Document {id:'a'})-[r:LINK]->(b) WITH a,b,p,[r] AS list,{edge:r} AS map SET a.ref=null WITH a,b,p,list,map,relationships(p)[0] AS missing SET a.ref=antfly.documentKey(b) RETURN missing.weight,list[0].weight,map.edge.weight,relationships(p)[0].weight"
            },
        )["rows"][0]
        assert row == [{"type": "number", "value": 1}] * 4
        assert database.sql("SELECT ref,x FROM docs WHERE _id='a'")["rows"] == [["b", "2"]]
    with af.open_hosted(aflite_path) as database:
        row = database.cypher_json("knowledge", {"query": "MATCH (a:Document {id:'a'})-[r:LINK]->(b) RETURN r.weight"})[
            "rows"
        ][0]
        assert row == [{"type": "number", "value": 1}]


def test_unicode_strings_filter_write_and_graph_table_survive_reopen(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT)")
        database.graph_catalog_json(
            {"action": "put", "name": "knowledge", "definition": {"labels": [{"name": "Document", "table": "docs"}]}}
        )
        result = database.cypher_json(
            "knowledge",
            {
                "query": "RETURN toLower($text),toUpper($text),trim($space),ltrim($space),rtrim($space),trim($nbsp)",
                "parameters": {"text": "ÉCOLE Straße ΟΣ 𐐀", "space": "\u2003hi\u3000", "nbsp": "\u00a0hi\u202f"},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == [
            "école straße ος 𐐨",
            "ÉCOLE STRASSE ΟΣ 𐐀",
            "hi",
            "hi\u3000",
            "\u2003hi",
            "\u00a0hi\u202f",
        ]
        result = database.cypher_json(
            "knowledge",
            {
                "query": "CREATE (n:Document {name:'ÉCOLE'}) WITH n WHERE toLower(n.name)='école' SET n.name=toUpper(trim($name)) RETURN n.name",
                "parameters": {"name": "\u2003Straße\u3000"},
            },
        )
        assert result["rows"] == [[{"type": "string", "value": "STRASSE"}]]
        database.cypher_json("knowledge", {"query": "CREATE (:Document {name:'ÉCOLE'})"})
    with af.open_hosted(aflite_path) as database:
        result = database.cypher_json(
            "knowledge", {"query": "MATCH (n:Document) WHERE toLower(n.name)='école' RETURN n.name"}
        )
        assert result["rows"] == [[{"type": "string", "value": "ÉCOLE"}]]
        assert database.sql(
            "SELECT g.name FROM GRAPH_TABLE(knowledge MATCH (n IS Document) WHERE lower(n.name)='école' COLUMNS (upper(n.name) AS name)) g"
        )["rows"] == [["ÉCOLE"]]


def test_scalar_errors_roll_back_and_numbers_preserve_cypher_semantics(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT)")
        database.graph_catalog_json(
            {"action": "put", "name": "knowledge", "definition": {"labels": [{"name": "Document", "table": "docs"}]}}
        )
        for value in (42, True, "text", [1]):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json(
                    "knowledge",
                    {
                        "query": "CREATE (:Document {name:'rollback'}) WITH $value AS x RETURN x.foo",
                        "parameters": {"value": value},
                    },
                )
            assert invalid.value.graph_code == "CypherTypeMismatch"
        for text in ("9223372036854775808", "-9223372036854775809", "9223372036854775807.1", "-9223372036854775808.1"):
            with pytest.raises(af.GraphError) as overflow:
                database.cypher_json(
                    "knowledge",
                    {
                        "query": "CREATE (:Document {name:'rollback'}) RETURN toInteger($value)",
                        "parameters": {"value": text},
                    },
                )
            assert overflow.value.graph_code == "CypherNumericOverflow"
        assert database.sql("SELECT count(*) FROM docs")["rows"] == [["0"]]
        row = database.cypher_json(
            "knowledge",
            {
                "query": "RETURN toInteger('9007199254740993.0'),toInteger('9223372036854775807.0'),toInteger('-9223372036854775808.0'),toString(1.0),toString(1e20)"
            },
        )["rows"][0]
        assert [cell["value"] for cell in row] == [
            "9007199254740993",
            "9223372036854775807",
            "-9223372036854775808",
            "1.0",
            "1.0E20",
        ]
        distinct = database.cypher_json(
            "knowledge", {"query": "UNWIND [1,1.0] AS x RETURN collect(DISTINCT toString(x))"}
        )["rows"][0][0]
        assert distinct == {
            "type": "list",
            "value": [{"type": "string", "value": "1"}, {"type": "string", "value": "1.0"}],
        }
        for values, expected in (
            ("[9223372036854775807,9223372036854775807]", "toFloat(9223372036854775807)"),
            ("[1e308,1e308]", "1e308"),
            ("[1e308,-1e308]", "0.0"),
        ):
            assert database.cypher_json("knowledge", {"query": f"UNWIND {values} AS x RETURN avg(x)={expected}"})[
                "rows"
            ] == [[{"type": "boolean", "value": True}]]
        database.cypher_json("knowledge", {"query": "CREATE (:Document {name:toString(1.0)})"})
        assert database.sql(
            "SELECT g.value,g.text FROM GRAPH_TABLE(knowledge MATCH (n IS Document) COLUMNS (toInteger('9007199254740993.0') AS value,toString(1.0) AS text)) g"
        )["rows"] == [["9007199254740993", "1.0"]]
    with af.open_hosted(aflite_path) as database:
        assert database.sql("SELECT name FROM docs")["rows"] == [["1.0"]]


def test_entity_properties_and_rebound_relationships_reject_before_writes(require_native, aflite_path):
    definition = {
        "labels": [{"name": "D", "table": "docs"}],
        "relationships": [
            {"name": "LINK", "index": "links", "source_label": "D", "target_label": "D", "stored_type": "link"}
        ],
    }
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT,payload JSONB)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        database.graph_catalog_json({"action": "put", "name": "g", "definition": definition})
        for query in (
            "CREATE (a:D) CREATE (b:D {payload:a}) RETURN b",
            "CREATE (n:D) SET n.payload=n RETURN n",
            "CREATE (n:D) SET n.payload={nested:[n]} RETURN n",
            "CREATE p=(a:D)-[r:LINK]->(b:D) SET a.payload=p RETURN a",
            "CREATE (a:D)-[r:LINK]->(b:D) SET r.payload=r RETURN r",
            "CREATE (a:D)-[r:LINK {payload:a}]->(b:D) RETURN r",
        ):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json("g", {"query": query})
            assert invalid.value.graph_code == "CypherInvalidPropertyValue"
            assert database.sql("SELECT count(*) FROM docs")["rows"] == [["0"]]
        for prefix in ("", "EXPLAIN "):
            for verb in ("CREATE", "MERGE"):
                with pytest.raises(af.GraphError) as conflict:
                    database.cypher_json(
                        "g", {"query": f"{prefix}MATCH (a:D)-[r:LINK]->(b) {verb} (a)-[r:LINK]->(b) RETURN r"}
                    )
                assert conflict.value.graph_code == "CypherVariableConflict"
        database.cypher_json("g", {"query": "CREATE (a:D {name:'a'})-[r:LINK {x:1,weight:'heavy'}]->(b:D {name:'b'})"})
        for query, code in (
            ("MATCH (n:D {name:'a'}) SET n.payload=n RETURN n.name", "CypherInvalidPropertyValue"),
            (
                "MATCH (a:D)-[r:LINK]->(b) MERGE (a)-[r:LINK]->(b) ON MATCH SET r.x=2 RETURN r.x",
                "CypherVariableConflict",
            ),
        ):
            with pytest.raises(af.GraphError) as invalid:
                database.cypher_json("g", {"query": query})
            assert invalid.value.graph_code == code
    with af.open_hosted(aflite_path) as database:
        assert database.sql("SELECT name,payload FROM docs ORDER BY name")["rows"] == [["a", None], ["b", None]]
        weight = database.sql(
            "SELECT g.weight FROM GRAPH_TABLE(g MATCH ()-[r IS LINK]->() COLUMNS(r.weight AS weight)) g"
        )
        assert weight["columns"] == [{"name": "weight", "type": "json"}]
        assert weight["rows"] == [["heavy"]]
        assert database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r.x"})["rows"] == [
            [{"type": "integer", "value": "1"}]
        ]


def test_filtered_self_loops_follow_declared_endpoint_predicates(require_native, aflite_path):
    labels = [
        {"name": "D", "table": "docs"},
        {"name": "Source", "table": "docs", "where": {"s": True}},
        {"name": "Target", "table": "docs", "where": {"t": True}},
    ]
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT,s BOOLEAN,t BOOLEAN)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        for graph, source, target in (("all", "D", "D"), ("filtered", "Source", "Target")):
            database.graph_catalog_json(
                {
                    "action": "put",
                    "name": graph,
                    "definition": {
                        "labels": labels,
                        "relationships": [
                            {
                                "name": "LINK",
                                "index": "links",
                                "source_label": source,
                                "target_label": target,
                                "stored_type": "link",
                            }
                        ],
                    },
                }
            )
        database.cypher_json(
            "all",
            {
                "query": "CREATE (a:D {name:'target',s:false,t:true})-[:LINK]->(a), (b:D {name:'both',s:true,t:true})-[:LINK]->(b), (c:D {name:'source',s:true,t:false})-[:LINK]->(c)"
            },
        )
    with af.open_hosted(aflite_path) as database:
        for pattern in ("(n:Target)<-[r:LINK]-(m)", "(n)-[r:LINK]-(m)", "(n:Source)-[r:LINK]->(m)"):
            assert database.cypher_json("filtered", {"query": f"MATCH {pattern} RETURN n.name"})["rows"] == [
                [{"type": "string", "value": "both"}]
            ]
        assert database.sql(
            "SELECT g.name FROM GRAPH_TABLE(filtered MATCH (n)<-[r IS LINK]-(m) COLUMNS(n.name AS name)) g"
        )["rows"] == [["both"]]


def test_graph_table_scalar_types_and_empty_wildcard_schema(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT,amount BIGINT)")
        database.graph_catalog_json(
            {"action": "put", "name": "g", "definition": {"labels": [{"name": "D", "table": "docs"}]}}
        )
        for prefix in ("", "EXPLAIN "):
            result = database.cypher_json("g", {"query": f"{prefix}MATCH (n:D) RETURN *"})
            assert result["columns"] == ["n"] and result["rows"] == []
        database.sql("INSERT INTO docs (name,amount) VALUES (' a ',-1),('b',2)")
        for expression, expected in (
            ("trim(n.name)", ["a", "b"]),
            ("ltrim(n.name)", ["a ", "b"]),
            ("rtrim(n.name)", [" a", "b"]),
            ("substring(n.name,1,1)", ["a", ""]),
            ("reverse(n.name)", [" a ", "b"]),
            ("coalesce(null,trim(n.name))", ["a", "b"]),
        ):
            result = database.sql(
                f"SELECT g.x || '!' FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS({expression} AS x,n.amount AS amount)) g ORDER BY g.amount"
            )
            assert result["rows"] == [[value + "!"] for value in expected]
        result = database.sql(
            "SELECT g.name FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(toBoolean('true') AS ok,n.name AS name)) g WHERE g.ok ORDER BY g.name"
        )
        assert result["rows"] == [[" a "], ["b"]]
        result = database.sql("SELECT sum(g.x) FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(abs(n.amount) AS x)) g")
        assert result["rows"] == [["3"]]
        for function in ("sum", "min", "max"):
            result = database.sql(
                f"SELECT g.x + 1 FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS({function}(n.amount) AS x)) g"
            )
            assert result["rows"] == [[{"sum": "2", "min": "0", "max": "3"}[function]]]


def test_union_inline_predicates_and_scalar_conversions_persist(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT,amount BIGINT)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        database.graph_catalog_json(
            {
                "action": "put",
                "name": "g",
                "definition": {
                    "labels": [{"name": "D", "table": "docs"}],
                    "relationships": [
                        {
                            "name": "LINK",
                            "index": "links",
                            "source_label": "D",
                            "target_label": "D",
                            "stored_type": "link",
                        }
                    ],
                },
            }
        )
        database.cypher_json(
            "g",
            {
                "query": "CREATE (a:D {name:'a',amount:1})-[:LINK {weight:1}]->(b:D {name:'b',amount:2})-[:LINK]->(c:D {name:'c',amount:3})"
            },
        )
        pattern = "MATCH p=(a:D {name:'a'})-[r:LINK]->(b)-[s:LINK]->(c) "
        for left, right in (("*", "a,b,c,r,s,p"), ("a,b,c,r,s,p", "*")):
            for union, count in (("UNION", 1), ("UNION ALL", 2)):
                result = database.cypher_json("g", {"query": f"{pattern}RETURN {left} {union} {pattern}RETURN {right}"})
                assert result["columns"] == ["a", "b", "c", "r", "s", "p"]
                assert len(result["rows"]) == count
                assert [cell["type"] for cell in result["rows"][0]] == [
                    "node",
                    "node",
                    "node",
                    "relationship",
                    "relationship",
                    "path",
                ]
                if count == 2:
                    assert result["rows"][0] == result["rows"][1]
        result = database.cypher_json("g", {"query": "MATCH (n:D {name:n.name}) RETURN n.name ORDER BY n.name LIMIT 3"})
        assert [row[0]["value"] for row in result["rows"]] == ["a", "b", "c"]
        nested = database.cypher_json(
            "g",
            {
                "query": "MATCH (a:D {amount:CASE WHEN EXISTS { MATCH (b) WHERE b.amount=1 } THEN 1 ELSE 2 END}),(b:D) RETURN a.name,b.name ORDER BY a.name,b.name"
            },
        )
        assert [[cell["value"] for cell in row] for row in nested["rows"]] == [["a", "a"], ["b", "b"], ["b", "c"]]
        result = database.cypher_json(
            "g",
            {
                "query": "MATCH (a:D {name:'a'})-[r:LINK {weight:b.amount-1}]->(b) SET a.amount=toInteger(toFloat(' 4.0d ')),r.ok=toBoolean(-1) RETURN a.amount,r.ok,'row-'+a.amount"
            },
        )
        assert result["rows"] == [
            [
                {"type": "integer", "value": "4"},
                {"type": "boolean", "value": True},
                {"type": "string", "value": "row-4"},
            ]
        ]
        for expression in ("'row-'+n.amount", "n.amount+'-row'", "'v='+toBoolean(n.amount)"):
            result = database.sql(
                f"SELECT g.x || '!' FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS ({expression} AS x)) g ORDER BY g.x"
            )
            expected = {
                "'row-'+n.amount": ["row-2!", "row-3!", "row-4!"],
                "n.amount+'-row'": ["2-row!", "3-row!", "4-row!"],
                "'v='+toBoolean(n.amount)": ["v=true!"] * 3,
            }[expression]
            assert result["rows"] == [[value] for value in expected]
        database.cypher_json(
            "g", {"query": "MATCH (n:D {name:'a'}) SET n.name='row-'+toInteger(toFloat(' 7.0F ')) RETURN n.name"}
        )
    with af.open_hosted(aflite_path) as database:
        assert database.sql("SELECT name,amount FROM docs ORDER BY name")["rows"] == [
            ["b", "2"],
            ["c", "3"],
            ["row-7", "4"],
        ]
        result = database.cypher_json("g", {"query": "MATCH (a:D {name:'row-7'})-[r:LINK]->(b) RETURN r.ok"})
        assert result["rows"] == [[{"type": "boolean", "value": True}]]


def _put_link_graph(database, name="g", predicate=None):
    database.graph_catalog_json(
        {
            "action": "put",
            "name": name,
            "definition": {
                "labels": [{"name": "D", "table": "docs", **({"where": predicate} if predicate else {})}],
                "relationships": [
                    {"name": "LINK", "index": "links", "source_label": "D", "target_label": "D", "stored_type": "link"}
                ],
            },
        }
    )


def test_shortest_paths_resolve_later_pattern_dependencies(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT,amount BIGINT)")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        _put_link_graph(database)
        database.cypher_json(
            "g",
            {
                "query": "CREATE (a:D {name:'a'})-[:LINK {weight:1}]->(b:D {name:'b'}),"
                "(a)-[:LINK {weight:2}]->(c:D {name:'c'})-[:LINK {weight:2}]->(b),"
                "(x:D {name:'x',amount:2})"
            },
        )
        for selector in ("shortestPath", "allShortestPaths"):
            path = f"p={selector}((a:D {{name:'a'}})-[:LINK*1..3 {{weight:x.amount}}]->(b:D {{name:'b'}}))"
            for patterns in (f"{path},(x:D {{name:'x'}})", f"(x:D {{name:'x'}}),{path}"):
                assert database.cypher_json("g", {"query": f"MATCH {patterns} RETURN length(p)"})["rows"] == [
                    [{"type": "integer", "value": "2"}]
                ]
                assert database.cypher_json("g", {"query": f"OPTIONAL MATCH {patterns} RETURN length(p)"})["rows"] == [
                    [{"type": "integer", "value": "2"}]
                ]


def test_nonincident_producer_deletion_updates_cypher_and_sql_views(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (name TEXT, kind TEXT)")
        database.sql("INSERT INTO docs (_id,name) VALUES ('a','a'),('b','b'),('claim','claim'),('other','other')")
        database.sql("UPDATE docs SET kind='vertex' WHERE _id IN ('a','b')")
        with database.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
            table.batch_json(
                {
                    "graph_writes": [
                        {
                            "index_name": "links",
                            "source": "a",
                            "target": "b",
                            "edge_type": "link",
                            "edge_id": "fact:1",
                            "owner_document": owner,
                        }
                        for owner in ("claim", "other")
                    ],
                    "sync_level": "full_index",
                }
            )
        _put_link_graph(database)
        count = "SELECT count(*) FROM GRAPH_TABLE(g MATCH ()-[r IS LINK]->() COLUMNS(r AS edge)) g"
        assert database.sql(count)["rows"] == [["2"]]
        _put_link_graph(database, name="vertices", predicate={"kind": "vertex"})
        nonvertex_count = count.replace("GRAPH_TABLE(g ", "GRAPH_TABLE(vertices ")
        assert database.sql(nonvertex_count)["rows"] == [["2"]]
        assert database.cypher_json(
            "g",
            {
                "query": "MATCH (n:D {name:'claim'}) SET n.kind='producer' WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"
            },
        )["rows"] == [[{"type": "integer", "value": "2"}]]
        with closing(database.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("UPDATE docs SET name='changed' WHERE _id='claim'")
            assert session.execute(count)["rows"] == [["2"]]
            assert session.execute(nonvertex_count)["rows"] == [["2"]]
            session.execute("ROLLBACK")
        with closing(database.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("DELETE FROM docs WHERE _id='claim'")
            assert session.execute(count)["rows"] == [["1"]]
            session.execute("ROLLBACK")
        assert database.sql(count)["rows"] == [["2"]]
        with pytest.raises(af.GraphError):
            database.cypher_json("g", {"query": "MATCH (n:D {name:'claim'}) DELETE n RETURN 1/0"})
        assert database.sql(count)["rows"] == [["2"]]
        result = database.cypher_json(
            "g",
            {"query": "MATCH (n:D {name:'claim'}) DELETE n WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"},
        )
        assert result["rows"] == [[{"type": "integer", "value": "1"}]]
        with database.open_table("docs") as table:
            table.run_until_idle()
        assert database.sql(count)["rows"] == [["1"]]


def _seed_artifact_links(database, extraction, *, mixed=False):
    if mixed:
        database.sql("CREATE TABLE docs (name TEXT, ref TEXT, kg JSONB)")
    else:
        database.create_table("docs", {})
    with database.open_table("docs") as table:
        table.add_index(
            {
                "name": "links",
                "type": "graph",
                "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
                "artifact": {
                    "name": "kg",
                    "kind": "asset",
                    "source": {"type": "field", "value": "kg"},
                    "content_type": "application/json",
                },
                "edge_types": [{"name": "mentions"}, {"name": "link", **({"field": "ref"} if mixed else {})}],
                "resolvers": [
                    {
                        "name": "entities",
                        "table": "docs",
                        "promote": False,
                        "source_artifact": "kg",
                        "resolution_artifact": "resolved",
                        "key_template": "entity/{{ uslug _entity.text }}",
                        "config_generation": 1,
                    }
                ],
            }
        )
        table.batch_json(
            {
                "inserts": {
                    "entity/a": {"name": "a", **({"ref": "entity/b", "kg": extraction} if mixed else {})},
                    "entity/b": {"name": "b"},
                    "claim": {"name": "claim", "kg": extraction},
                    "other": {"name": "other", "kg": extraction},
                }
            }
        )
        table.run_until_idle()
    _put_link_graph(database)


def test_artifact_assertions_preserve_ownership_and_producer_identity(require_native, aflite_path):
    options = af.OpenOptions(profile=af.Profile.NATIVE, generated_enrichment_replay=True)
    extraction = {
        "entities": [{"id": "a", "label": "thing", "text": "A"}, {"id": "b", "label": "thing", "text": "B"}],
        "relations": [{"type": "link", "source": "a", "target": "b"}],
    }
    with af.create_with_options(aflite_path, options) as database:
        _seed_artifact_links(database, extraction)
        relationships = database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r"})["rows"]
        assert len(relationships) == 2
        assert {row[0]["value"]["ownership"] for row in relationships} == {"artifact"}
        assert len({row[0]["value"]["identity"]["key"] for row in relationships}) == 2
        assert {row[0]["value"]["producer"]["key"] for row in relationships} == {"claim", "other"}
        assert database.cypher_json(
            "g",
            {
                "query": "MATCH (n:D {name:'claim'}) SET n.note='updated' WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"
            },
        )["rows"] == [[{"type": "integer", "value": "2"}]]
        # A legacy direct contender shares tuple-level private contributor state;
        # it must not provide a mutation path that erases the managed assertions.
        with database.open_table("docs") as table:
            table.batch_json(
                {
                    "graph_writes": [
                        {"index_name": "links", "source": "entity/a", "target": "entity/b", "edge_type": "link"}
                    ],
                    "sync_level": "full_index",
                }
            )
        relationships = database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r"})["rows"]
        assert len(relationships) == 3
        assert {row[0]["value"]["ownership"] for row in relationships} == {"artifact"}
        for action in ("SET r.test=1 RETURN r.test", "REMOVE r.test RETURN r", "DELETE r"):
            with pytest.raises(af.GraphError) as derived:
                database.cypher_json("g", {"query": f"MATCH ()-[r:LINK]->() {action}"})
            assert derived.value.graph_code == "CypherDerivedRelationshipMutation"
        # Explicit relationships can coexist in an artifact-backed index.
        database.cypher_json(
            "g",
            {
                "query": "MATCH (a:D {name:'a'}),(b:D {name:'b'}) CREATE (a)-[r:LINK {test:1}]->(b) SET r.test=2 RETURN r.test"
            },
        )
        assert database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() WHERE r.test=2 RETURN r.test"})["rows"] == [
            [{"type": "integer", "value": "2"}]
        ]
        with closing(database.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("DELETE FROM docs WHERE _id='claim'")
            assert session.execute("SELECT count(*) FROM GRAPH_TABLE(g MATCH ()-[r IS LINK]->() COLUMNS(r AS edge)) g")[
                "rows"
            ] == [["3"]]
            session.execute("ROLLBACK")
        result = database.cypher_json(
            "g",
            {"query": "MATCH (n:D {name:'claim'}) DELETE n WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"},
        )
        assert result["rows"] == [[{"type": "integer", "value": "3"}]]
        with database.open_table("docs") as table:
            table.run_until_idle()
        assert len(database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r"})["rows"]) == 3
    with af.open_with_options(aflite_path, options) as database:
        relationships = database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r"})["rows"]
        assert {row[0]["value"]["ownership"] for row in relationships} == {"artifact", "explicit"}
        # A single physical relationship can have more producers than one
        # private adjacency page. LIMIT must not require repeated full scans of
        # all contributors or charge a quadratic amount of physical work.
        with database.open_table("docs") as table:
            table.batch_json({"inserts": {f"fan-in/{i}": {"kg": extraction} for i in range(2048)}})
            table.run_until_idle()
        result = database.cypher_json("g", {"query": "MATCH (a:D {name:'a'})-[r:LINK]->() RETURN r LIMIT 1"})
        assert len(result["rows"]) == 1


def test_fields_and_artifacts_from_one_producer_have_independent_postimages(require_native, aflite_path):
    options = af.OpenOptions(profile=af.Profile.NATIVE, generated_enrichment_replay=True)
    extraction = {
        "entities": [{"id": "a", "label": "thing", "text": "A"}, {"id": "b", "label": "thing", "text": "B"}],
        "relations": [{"type": "link", "source": "a", "target": "b"}],
    }
    with af.create_with_options(aflite_path, options) as database:
        _seed_artifact_links(database, extraction, mixed=True)
        rows = database.cypher_json("g", {"query": "MATCH ()-[r:LINK]->() RETURN r"})["rows"]
        assert len(rows) == 4
        assert len({row[0]["value"]["identity"]["key"] for row in rows}) == 4
        assert sorted(row[0]["value"]["ownership"] for row in rows) == ["artifact"] * 3 + ["field"]
        count = "SELECT count(*) FROM GRAPH_TABLE(g MATCH ()-[r IS LINK]->() COLUMNS(r AS edge)) g"
        with closing(database.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("UPDATE docs SET ref=NULL WHERE _id='entity/a'")
            assert session.execute(count)["rows"] == [["3"]]
            session.execute("ROLLBACK")
        result = database.cypher_json(
            "g", {"query": "MATCH (a:D {name:'a'}) SET a.ref=null WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN r"}
        )
        assert len(result["rows"]) == 3
        assert {row[0]["value"]["ownership"] for row in result["rows"]} == {"artifact"}
        with database.open_table("docs") as table:
            table.run_until_idle()
        assert database.sql(count)["rows"] == [["3"]]


def test_computed_graph_values_are_bounded_before_delivery_and_commit(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT)")
        database.graph_catalog_json(
            {"action": "put", "name": "g", "definition": {"labels": [{"name": "D", "table": "docs"}]}}
        )
        for query in (
            "RETURN reduce(a=[],x IN range(1,10000)|[a]) AS nested",
            "CREATE (n:D {id:'rollback'}) WITH n,reduce(a=[],x IN range(1,10000)|[a]) AS a RETURN a",
        ):
            with pytest.raises(af.GraphError) as failed:
                database.cypher_json("g", {"query": query})
            assert failed.value.graph_code == "CypherValueDepthExceeded"
        assert database.cypher_json("g", {"query": "MATCH (n:D) RETURN count(n)"})["rows"][0][0]["value"] == "0"
        response = database.cypher_json("g", {"query": "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a RETURN a=a"})
        assert response["rows"][0][0] == {"type": "boolean", "value": True}
        assert response["stats"]["intermediate_states"] < 1000
        for query in (
            "UNWIND range(1,8000) AS x RETURN x,count(*)",
            "UNWIND range(9223372036854771800,9223372036854775799) AS x RETURN x,count(*)",
            "UNWIND range(1,8000) AS x RETURN x UNION RETURN 0 AS x",
        ):
            response = database.cypher_json("g", {"query": query})
            assert response["complete"]
            assert response["stats"]["intermediate_states"] < 200000
        database.sql("INSERT INTO docs (id) VALUES ('a')")
        from antfly_embedded._sql import SQLStateError

        with pytest.raises(SQLStateError) as limited:
            database.sql("SELECT g.x FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(range(1,1000000) AS x)) g")
        assert limited.value.sqlstate == "54000"
        nested = database.sql("SELECT g.nodes FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(collect(n) AS nodes)) g")[
            "rows"
        ][0][0]
        assert isinstance(nested, list) and nested[0]["type"] == "node"


def test_bulk_mutations_and_string_scans_share_the_statement_budget(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT)")
        database.graph_catalog_json(
            {"action": "put", "name": "g", "definition": {"labels": [{"name": "D", "table": "docs"}]}}
        )
        create = "UNWIND range(1,1000) AS x CREATE (n:D {id:toString(x)}) RETURN count(*)"
        result = database.cypher_json("g", {"query": create, "limits": {"intermediate_states": 50000}})
        assert result["rows"][0][0]["value"] == "1000"
        result = database.cypher_json("g", {"query": "MATCH (n:D) DELETE n RETURN count(n)"})
        assert result["rows"][0][0]["value"] == "1000"
        assert result["stats"]["intermediate_states"] < 50000
        with pytest.raises(af.GraphError) as limited:
            database.cypher_json("g", {"query": create, "limits": {"intermediate_states": 20000}})
        assert limited.value.graph_code == "CypherStateBudgetExceeded"
        assert database.cypher_json("g", {"query": "MATCH (n:D) RETURN count(n)"})["rows"][0][0]["value"] == "0"
        text = "x" * 400000
        cached = database.cypher_json(
            "g",
            {
                "query": "RETURN reduce(a=0,i IN range(1,1000)|a+size(ltrim($s)))",
                "parameters": {"s": text},
                "limits": {"intermediate_states": 30000},
            },
        )
        assert cached["rows"][0][0]["value"] == "400000000"
        with pytest.raises(af.GraphError) as limited:
            database.cypher_json(
                "g",
                {
                    "query": "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN $s CONTAINS 'z' THEN 1 ELSE 0 END)",
                    "parameters": {"s": text},
                    "limits": {"intermediate_states": 30000},
                },
            )
        assert limited.value.graph_code == "CypherStateBudgetExceeded"
        database.sql("INSERT INTO docs (id) VALUES ('one')")
        assert database.sql(
            "SELECT g.characters FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(size(ltrim($1)) AS characters)) g",
            [text],
        )["rows"] == [["400000"]]


def test_native_capture_and_bulk_delete_fit_the_default_memory_ceiling(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, age BIGINT)")
        database.graph_catalog_json(
            {"action": "put", "name": "g", "definition": {"labels": [{"name": "D", "table": "docs"}]}}
        )
        database.cypher_json("g", {"query": "UNWIND range(1,8000) AS x CREATE (n:D {id:toString(x)}) RETURN count(*)"})
        changed = database.cypher_json(
            "g", {"query": "MATCH (n:D) WITH n,[n,n] AS aliases SET n.age=1 RETURN sum(aliases[0].age)"}
        )
        assert changed["rows"][0][0]["value"] == "8000"
        removed = database.cypher_json(
            "g", {"query": "MATCH (n:D) WITH n,[n,n] AS aliases REMOVE n.age RETURN count(aliases[0].age)"}
        )
        assert removed["rows"][0][0]["value"] == "0"
        deleted = database.cypher_json("g", {"query": "MATCH (n:D) DELETE n RETURN count(n)"})
        assert deleted["rows"][0][0]["value"] == "8000"
        assert deleted["stats"]["intermediate_states"] < 400000
        assert database.sql("SELECT count(*) FROM docs")["rows"] == [["0"]]


def test_nested_label_predicates_share_capture_and_runtime_work_limits(require_native, aflite_path):
    meta = {f"field{i}": i for i in range(4000)}
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, meta JSONB)")
        database.sql("INSERT INTO docs (id,meta) VALUES ('one','" + json.dumps(meta) + "'::jsonb)")
        database.graph_catalog_json(
            {
                "action": "put",
                "name": "g",
                "definition": {"labels": [{"name": "D", "table": "docs", "where": {"meta": meta}}]},
            }
        )
        with pytest.raises(af.GraphError) as limited:
            database.cypher_json("g", {"query": "MATCH (n:D) RETURN count(*)", "limits": {"intermediate_states": 100}})
        assert limited.value.graph_code == "CypherStateBudgetExceeded"
        result = database.cypher_json("g", {"query": "MATCH (n:D) RETURN count(*)"})
        assert result["rows"][0][0]["value"] == "1"
        assert 4000 < result["stats"]["intermediate_states"] < 400000


def test_repeated_updates_of_shared_aliases_scale_linearly(require_native, aflite_path):
    with af.create_hosted(aflite_path) as database:
        database.sql("CREATE TABLE docs (id TEXT, age BIGINT)")
        database.sql("INSERT INTO docs (id,age) VALUES ('one',0)")
        database.graph_catalog_json(
            {"action": "put", "name": "g", "definition": {"labels": [{"name": "D", "table": "docs"}]}}
        )
        previous = 0
        for count in [400, 800, 1600]:
            database.sql("UPDATE docs SET age=0 WHERE id='one'")
            result = database.cypher_json(
                "g",
                {
                    "query": f"MATCH (n:D) WITH n,[{{node:n}}] AS aliases,properties(n) AS snapshot UNWIND range(1,{count}) AS i SET n.age=n.age+1 RETURN count(*),sum(aliases[0].node.age),sum(snapshot.age)"
                },
            )
            assert [cell["value"] for cell in result["rows"][0]] == [str(count), str(count * count), "0"]
            work = result["stats"]["intermediate_states"]
            if previous:
                assert work < previous * 3
            previous = work
