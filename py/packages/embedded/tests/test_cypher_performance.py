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

"""Physical-work regressions through the real public embedded boundary."""

from contextlib import closing

import pytest

import antfly_embedded as af


@pytest.fixture(params=[af.Storage.LITE, af.Storage.DIRECTORY])
def graph_db(require_native, tmp_path, request):
    storage = request.param
    path = tmp_path / ("graph.aflite" if storage == af.Storage.LITE else "graph")
    opener = af.create_with_options if storage == af.Storage.LITE else af.open_with_options
    with closing(opener(path, af.OpenOptions(storage=storage))) as db:
        db.sql("CREATE TABLE docs (id TEXT,score BIGINT)")
        with db.open_table("docs") as table:
            table.add_index({"name": "links", "kind": "graph", "config_json": "{}"})
        db.graph_catalog_json(
            {
                "action": "put",
                "name": "g",
                "definition": {
                    "labels": [{"name": name, "table": "docs"} for name in ("D", "Other")],
                    "relationships": [
                        {"name": "R", "index": "links", "source_label": "D", "target_label": "D", "stored_type": "link"}
                    ],
                },
            }
        )
        yield db


def populate(db, count):
    for start in range(0, count, 500):
        db.sql(
            "INSERT INTO docs (_id,id,score) VALUES "
            + ",".join(f"('n{i}','n{i}',{i})" for i in range(start, min(start + 500, count)))
        )


def test_point_reads_and_writes_do_not_capture_the_graph(graph_db):
    db = graph_db
    populate(db, 1000)
    constant = db.cypher_json("g", {"query": "RETURN 1", "limits": {"scanned_anchors": 0, "explored_edges": 0}})
    assert constant["stats"]["scanned_anchors"] == 0
    anchor = "MATCH (n:D) WHERE antfly.documentKey(n)='n0' "
    for tail in ("RETURN n.score", "SET n.score=7 RETURN n.score"):
        result = db.cypher_json("g", {"query": anchor + tail, "limits": {"scanned_anchors": 1}})
        assert result["complete"]
        assert result["stats"]["scanned_anchors"] == 1
    assert db.sql("SELECT score FROM docs WHERE _id='n0'")["rows"] == [["7"]]


def test_shared_labels_and_terminal_operators_have_bounded_memory(graph_db):
    db = graph_db
    count = 2000
    populate(db, count)
    limits = {"scanned_anchors": count, "memory_bytes": 2 << 20}
    aggregate = db.cypher_json(
        "g",
        {"query": "MATCH (n) RETURN count(*),sum(n.score),avg(n.score),min(n.score),max(n.score)", "limits": limits},
    )
    assert aggregate["stats"]["scanned_anchors"] == count
    assert [cell["value"] for cell in aggregate["rows"][0]] == [
        str(count),
        str(count * (count - 1) // 2),
        pytest.approx((count - 1) / 2),
        "0",
        str(count - 1),
    ]
    top = db.cypher_json(
        "g",
        {
            "query": "MATCH (n) RETURN {id:n.id,score:n.score} AS row ORDER BY row.score DESC SKIP 2 LIMIT 3",
            "limits": limits,
        },
    )
    assert [row[0]["value"]["score"]["value"] for row in top["rows"]] == [
        str(count - 3),
        str(count - 4),
        str(count - 5),
    ]
    assert top["stats"]["scanned_anchors"] == count
    assert db.sql("SELECT g.total FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(count(*) AS total)) g")["rows"] == [
        [str(count)]
    ]

    # Merge staged keys on both sides of every native page boundary. Untouched
    # rows remain page-owned even when GRAPH_TABLE has a SQL overlay.
    with closing(db.sql_session()) as session:
        session.execute("BEGIN")
        session.execute("DELETE FROM docs WHERE _id='n128'")
        session.execute("UPDATE docs SET score=-7 WHERE _id='n127'")
        session.execute("INSERT INTO docs (_id,id,score) VALUES ('a','a',-10),('z','z',3000)")
        assert session.execute("SELECT g.total FROM GRAPH_TABLE(g MATCH (n IS D) COLUMNS(count(*) AS total)) g")[
            "rows"
        ] == [[str(count + 1)]]
        assert session.execute(
            "SELECT g.score FROM GRAPH_TABLE(g MATCH (n IS D) WHERE n.id='n127' COLUMNS(n.score AS score)) g"
        )["rows"] == [["-7"]]
        session.execute("ROLLBACK")


def test_shortest_path_does_not_enumerate_longer_paths(graph_db):
    db = graph_db
    count = 18
    patterns = [f"(n{i}:D {{id:'n{i}'}})" for i in range(count)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(count) for j in range(i + 1, count)]
    endpoints = db.cypher_json(
        "g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(n0),antfly.documentKey(n17)"}
    )["rows"][0]
    first, last = (cell["value"] for cell in endpoints)
    for selector in ("shortestPath", "allShortestPaths"):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH p={selector}((a:D)-[:R*1..18]->(b:D)) WHERE antfly.documentKey(a)=$key AND b.id='n17' RETURN length(p)",
                "parameters": {"key": first},
                "limits": {"scanned_anchors": 1, "explored_edges": 500, "memory_bytes": 4 << 20},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": "1"}]]
        assert result["stats"]["explored_edges"] == count * (count - 1) // 2
        bound = db.cypher_json(
            "g",
            {
                "query": f"MATCH (a:D) WHERE antfly.documentKey(a)=$first MATCH (b:D) WHERE antfly.documentKey(b)=$last MATCH p={selector}((a)-[:R*1..18]->(b)) RETURN length(p)",
                "parameters": {"first": first, "last": last},
                "limits": {"scanned_anchors": 2, "explored_edges": count - 1, "memory_bytes": 4 << 20},
            },
        )
        assert bound["rows"] == [[{"type": "integer", "value": "1"}]]
        assert bound["stats"]["explored_edges"] == count - 1


def test_lazy_overlay_discovers_incident_edges_before_delete(graph_db):
    db = graph_db
    db.cypher_json("g", {"query": "CREATE (a:D {id:'a'})-[:R]->(b:D {id:'b'})"})
    with pytest.raises(af.GraphError) as failure:
        db.cypher_json("g", {"query": "MATCH (n:D {id:'a'}) DELETE n"})
    assert failure.value.graph_code == "CypherNodeHasRelationships"
    db.cypher_json("g", {"query": "MATCH (n:D {id:'a'}) DETACH DELETE n"})
    assert db.cypher_json("g", {"query": "MATCH ()-[r:R]->() RETURN r"})["rows"] == []


def test_projection_chains_and_grouped_reducers_remain_bounded(graph_db):
    db = graph_db
    populate(db, 2000)
    limits = {"memory_bytes": 2 << 20}
    for query, expected in [
        ("MATCH (n:D) WITH n RETURN count(*)", [["2000"]]),
        ("MATCH (n:D) RETURN count(*)+1", [["2001"]]),
        ("MATCH (n:D) WITH count(*) AS total RETURN total+1", [["2001"]]),
        ("MATCH (n:D) WITH n.score AS s WHERE s>=1000 RETURN sum(s)", [["1499500"]]),
        ("MATCH (n:D) RETURN n.score%2 AS bucket,count(*) AS total ORDER BY bucket", [["0", "1000"], ["1", "1000"]]),
    ]:
        result = db.cypher_json("g", {"query": query, "limits": limits})
        assert [[cell["value"] for cell in row] for row in result["rows"]] == expected
        assert result["stats"]["memory_bytes"] < 2 << 20


def test_property_seeks_use_indexes_and_preserve_paging(graph_db):
    db = graph_db
    populate(db, 2000)
    db.sql("CREATE INDEX docs_id ON docs(id)")
    for condition in ("{id:'n1999'}", ""):
        suffix = "" if condition else " WHERE n.id='n1999'"
        result = db.cypher_json(
            "g", {"query": f"MATCH (n:D {condition}){suffix} RETURN count(*)", "limits": {"scanned_anchors": 4}}
        )
        assert result["rows"] == [[{"type": "integer", "value": "1"}]]
        assert result["stats"]["scanned_anchors"] <= 4
    result = db.cypher_json(
        "g",
        {
            "query": "MATCH (n:D {id:'n1999'}) SET n.id='renamed' WITH n MATCH (m:D {id:'renamed'}) RETURN m.score",
            "limits": {"scanned_anchors": 4},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "1999"}]]
    db.sql("UPDATE docs SET id='same' WHERE score<400")
    # The id index supplies many candidates rejected by the score residual.
    # Empty physical pages must still advance their index continuation.
    result = db.cypher_json(
        "g",
        {
            "query": "MATCH (n:D {id:'same',score:399}) RETURN n.score",
            "limits": {"scanned_anchors": 400},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "399"}]]
    assert result["stats"]["scanned_anchors"] == 400
    # A composite index orders candidates by its suffix, rather than document
    # key. Continuations must retain that native order across multiple pages.
    db.sql("DROP INDEX docs_id ON docs")
    db.sql("CREATE INDEX docs_id_score ON docs(id,score)")
    db.sql("UPDATE docs SET id='same' WHERE score<400")
    result = db.cypher_json(
        "g", {"query": "MATCH (n:D {id:'same'}) RETURN count(*),sum(n.score)", "limits": {"scanned_anchors": 450}}
    )
    assert [cell["value"] for cell in result["rows"][0]] == ["400", "79800"]
    # Pushdown remains only a candidate restriction; missing properties and
    # mixed-type comparisons retain Cypher semantics.
    assert db.cypher_json("g", {"query": "MATCH (n:D {missing:'x'}) RETURN count(*)"})["rows"] == [
        [{"type": "integer", "value": "0"}]
    ]


def test_all_shortest_paths_share_expansion_of_tied_prefixes(graph_db):
    db = graph_db
    layers = 8
    patterns = ["(a:D)", "(z:D)"] + [f"(n{i}_{j}:D)" for i in range(layers) for j in range(2)]
    patterns += [f"(a)-[:R]->(n0_{j})" for j in range(2)]
    patterns += [f"(n{i}_{j})-[:R]->(n{i + 1}_{k})" for i in range(layers - 1) for j in range(2) for k in range(2)]
    result = db.cypher_json(
        "g",
        {
            "query": "CREATE "
            + ",".join(patterns)
            + " RETURN antfly.documentKey(a),antfly.documentKey(z),antfly.documentKey(n7_0)"
        },
    )
    first, isolated, reachable = [cell["value"] for cell in result["rows"][0]]
    edges = 2 + 4 * (layers - 1)
    for target, count in ((isolated, 0), (reachable, 1 << (layers - 1))):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D) WHERE antfly.documentKey(a)=$first MATCH (z:D) WHERE antfly.documentKey(z)=$last MATCH p=allShortestPaths((a)-[:R*1..16]->(z)) RETURN count(*)",
                "parameters": {"first": first, "last": target},
                "limits": {"explored_edges": edges},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(count)}]]
        assert result["stats"]["explored_edges"] == edges


def test_index_candidates_preserve_cypher_numeric_and_type_semantics(graph_db):
    db = graph_db
    db.sql("CREATE TABLE amounts (amount NUMERIC)")
    db.sql("CREATE INDEX amount_index ON amounts(amount)")
    with db.open_table("amounts") as table:
        table.batch_json('{"inserts":{"one":{"amount":100000000000000000000.1}}}')
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "amounts",
            "definition": {"labels": [{"name": "A", "table": "amounts"}], "relationships": []},
        }
    )
    # NUMERIC's JSON projection rounds to a Cypher float. An exact-decimal
    # index restriction would exclude the row that the residual comparison admits.
    result = db.cypher_json("amounts", {"query": "MATCH (n:A) WHERE n.amount=1e20 RETURN count(*)"})
    assert result["rows"] == [[{"type": "integer", "value": "1"}]]
    populate(db, 300)
    db.sql("CREATE INDEX docs_score ON docs(score)")
    assert db.cypher_json("g", {"query": "MATCH (n:D {score:'nan'}) RETURN count(*)"})["rows"] == [
        [{"type": "integer", "value": "0"}]
    ]
    assert db.cypher_json("g", {"query": "MATCH (n:D {score:2.0}) RETURN n.id"})["rows"] == [
        [{"type": "string", "value": "n2"}]
    ]


def test_indexed_ranges_and_sparse_sets_keep_exact_cypher_residuals(graph_db):
    db = graph_db
    populate(db, 2000)
    db.sql("CREATE INDEX docs_score ON docs(score)")
    for predicate, anchors, expected in (
        ("n.score>=1999", 1, [1999]),
        ("1999<=n.score", 1, [1999]),
        ("n.score>1998 AND n.score<2000", 1, [1999]),
        ("1998<n.score<2000", 1, [1999]),
        ("n.score IN [1999,1,1999,null]", 2, [1, 1999]),
        ("n.score IN []", 0, []),
        ("n.score IN [null]", 0, []),
        ("n.score IN [1999,'incompatible']", 2000, [1999]),
        ("n.score>=1999.0", 2000, [1999]),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (n:D) WHERE {predicate} RETURN n.score ORDER BY n.score",
                "limits": {"scanned_anchors": anchors},
            },
        )
        assert [int(row[0]["value"]) for row in result["rows"]] == expected
        assert result["stats"]["scanned_anchors"] == anchors
    result = db.cypher_json(
        "g",
        {
            "query": "MATCH (n:D) WHERE n.score>=100 AND n.score<1500 RETURN count(*)",
            "limits": {"scanned_anchors": 1400, "memory_bytes": 2 << 20},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "1400"}]]
    assert result["stats"]["scanned_anchors"] == 1400
    with closing(db.sql_session()) as session:
        session.execute("BEGIN")
        session.execute("UPDATE docs SET score=5000 WHERE _id='n0'")
        session.execute("UPDATE docs SET score=-1 WHERE _id='n1999'")
        result = session.execute(
            "SELECT g.id FROM GRAPH_TABLE(g MATCH (n IS D) WHERE n.score>=1999 COLUMNS(n.id AS id)) g"
        )
        assert result["rows"] == [["n0"]]
        session.execute("ROLLBACK")


def test_traversals_stream_demand_top_k_and_reductions(graph_db):
    db = graph_db
    count = 400
    patterns = ["(a:D)"] + [f"(n{i}:D {{score:{i}}})" for i in range(count)]
    patterns += [f"(a)-[:R]->(n{i})" for i in range(count)]
    key = db.cypher_json("g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(a)"})["rows"][0][
        0
    ]["value"]
    prefix = "MATCH (a:D) WHERE antfly.documentKey(a)=$key MATCH (a)-[:R]->(b) "
    limited = db.cypher_json(
        "g",
        {
            "query": prefix + "RETURN b.score LIMIT 1",
            "parameters": {"key": key},
            "limits": {"explored_edges": 1},
        },
    )
    assert len(limited["rows"]) == 1
    assert limited["stats"]["explored_edges"] == 1
    for tail, expected in (
        ("RETURN count(*),sum(b.score)", [["400", "79800"]]),
        ("WITH b.score%2 AS parity RETURN parity,count(*) ORDER BY parity", [["0", "200"], ["1", "200"]]),
        ("RETURN b.score ORDER BY b.score DESC LIMIT 3", [["399"], ["398"], ["397"]]),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": prefix + tail,
                "parameters": {"key": key},
                "limits": {"memory_bytes": 2 << 20, "explored_edges": count},
            },
        )
        assert [[cell["value"] for cell in row] for row in result["rows"]] == expected


def test_shortest_operator_uses_bound_comma_inputs_and_counts_the_dag(graph_db):
    db = graph_db
    layers = 16
    patterns = ["(s:D)", "(z:D)"] + [f"(v{i}_{j}:D)" for i in range(layers) for j in range(2)]
    patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
    patterns += [f"(v{i}_{j})-[:R]->(v{i + 1}_{k})" for i in range(layers - 1) for j in range(2) for k in range(2)]
    keys = [
        cell["value"]
        for cell in db.cypher_json(
            "g",
            {
                "query": "CREATE "
                + ",".join(patterns)
                + f" RETURN antfly.documentKey(s),antfly.documentKey(z),antfly.documentKey(v{layers - 1}_0)"
            },
        )["rows"][0]
    ]
    edges = 2 + 4 * (layers - 1)
    for target, expected in ((keys[1], 0), (keys[2], 1 << (layers - 1))):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=allShortestPaths((s)-[:R*1..32]->(z)) RETURN count(*)",
                "parameters": {"s": keys[0], "z": target},
                "limits": {"explored_edges": edges, "memory_bytes": 2 << 20, "intermediate_states": 2000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
    limited = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=allShortestPaths((s)-[:R*1..32]->(z)) RETURN length(p) LIMIT 1",
            "parameters": {"s": keys[0], "z": keys[2]},
            "limits": {"explored_edges": edges, "memory_bytes": 2 << 20, "intermediate_states": 2000},
        },
    )
    assert limited["rows"] == [[{"type": "integer", "value": str(layers)}]]
    result = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D),(z:D),p=shortestPath((s)-[:R*1..32]->(z)) WHERE antfly.documentKey(s)=$s AND antfly.documentKey(z)=$z RETURN count(*)",
            "parameters": {"s": keys[0], "z": keys[1]},
            "limits": {"explored_edges": edges, "memory_bytes": 2 << 20},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "0"}]]
    assert result["stats"]["explored_edges"] == edges


def test_range_seeks_do_not_round_integer_bounds_on_float_columns(graph_db):
    db = graph_db
    db.sql("CREATE TABLE doubles (n DOUBLE PRECISION)")
    with db.open_table("doubles") as table:
        table.batch_json({"inserts": {"one": {"n": 9007199254740992.0}}})
    db.sql("CREATE INDEX doubles_n ON doubles(n)")
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "float_graph",
            "definition": {"labels": [{"name": "F", "table": "doubles"}], "relationships": []},
        }
    )
    result = db.cypher_json("float_graph", {"query": "MATCH (n:F) WHERE n.n<9007199254740993 RETURN count(*)"})
    assert result["rows"] == [[{"type": "integer", "value": "1"}]]


def test_dag_count_reports_overflow_only_for_qualifying_endpoints(graph_db):
    db = graph_db
    layers = 64
    patterns = ["(s:D)", "(z:D)"] + [f"(v{i}_{j}:D)" for i in range(layers) for j in range(2)]
    patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
    patterns += [f"(v{i}_{j})-[:R]->(v{i + 1}_{k})" for i in range(layers - 1) for j in range(2) for k in range(2)]
    keys = [
        cell["value"]
        for cell in db.cypher_json(
            "g",
            {
                "query": "CREATE "
                + ",".join(patterns)
                + f" RETURN antfly.documentKey(s),antfly.documentKey(z),antfly.documentKey(v{layers - 1}_0)"
            },
        )["rows"][0]
    ]
    query = "MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=allShortestPaths((s)-[:R*1..64]->(z)) RETURN count(*)"
    request = {"query": query, "parameters": {"s": keys[0], "z": keys[1]}, "limits": {"intermediate_states": 10000}}
    assert db.cypher_json("g", request)["rows"] == [[{"type": "integer", "value": "0"}]]
    request["parameters"]["z"] = keys[2]
    with pytest.raises(af.GraphError) as overflow:
        db.cypher_json("g", request)
    assert overflow.value.graph_code == "CypherNumericOverflow"
