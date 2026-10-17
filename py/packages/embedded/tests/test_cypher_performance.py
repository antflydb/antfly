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


@pytest.mark.parametrize("min_hops", [1, 2, 6])
def test_shortest_operator_uses_bound_comma_inputs_and_counts_the_dag(graph_db, min_hops):
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
                "query": f"MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=allShortestPaths((s)-[:R*{min_hops}..32]->(z)) RETURN count(*)",
                "parameters": {"s": keys[0], "z": target},
                "limits": {"explored_edges": edges, "memory_bytes": 2 << 20, "intermediate_states": 2000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
    limited = db.cypher_json(
        "g",
        {
            "query": f"MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=allShortestPaths((s)-[:R*{min_hops}..32]->(z)) RETURN length(p) LIMIT 1",
            "parameters": {"s": keys[0], "z": keys[2]},
            "limits": {"explored_edges": edges, "memory_bytes": 2 << 20, "intermediate_states": 2000},
        },
    )
    assert limited["rows"] == [[{"type": "integer", "value": str(layers)}]]
    result = db.cypher_json(
        "g",
        {
            "query": f"MATCH (s:D),(z:D),p=shortestPath((s)-[:R*{min_hops}..32]->(z)) WHERE antfly.documentKey(s)=$s AND antfly.documentKey(z)=$z RETURN count(*)",
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
    request["query"] = (
        "MATCH (s:D) WHERE antfly.documentKey(s)=$s MATCH (z:D) WHERE antfly.documentKey(z)=$z MATCH p=(s)-[:R*1..64]->(z) RETURN count(*)"
    )
    request["parameters"]["z"] = keys[1]
    assert db.cypher_json("g", request)["rows"] == [[{"type": "integer", "value": "0"}]]
    request["parameters"]["z"] = keys[2]
    with pytest.raises(af.GraphError) as overflow:
        db.cypher_json("g", request)
    assert overflow.value.graph_code == "CypherNumericOverflow"


def test_selective_right_endpoint_drives_traversal(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    patterns = [f"(n{i}:D {{id:'n{i}'}})" for i in range(201)]
    patterns += [f"(n{i})-[:R]->(n{i + 1})" for i in range(200)]
    key = db.cypher_json("g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(n200)"})["rows"][
        0
    ][0]["value"]
    for predicate in ("{id:'n200'}", ""):
        where = "" if predicate else f" WHERE antfly.documentKey(b)='{key}'"
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH p=(a:D)-[:R]->(b:D {predicate}){where} RETURN a.id,b.id,[n IN nodes(p)|n.id]",
                "limits": {"scanned_anchors": 1, "explored_edges": 1},
            },
        )
        assert [cell["value"] for cell in result["rows"][0][:2]] == ["n199", "n200"]
        assert [cell["value"] for cell in result["rows"][0][2]["value"]] == ["n199", "n200"]
        assert result["stats"]["scanned_anchors"] == 1
        assert result["stats"]["explored_edges"] == 1


def test_exists_propagates_edge_demand(graph_db):
    db = graph_db
    patterns = ["(a:D)"] + [f"(n{i}:D {{score:{i}}})" for i in range(200)]
    patterns += [f"(a)-[:R]->(n{i})" for i in range(200)]
    key = db.cypher_json("g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(a)"})["rows"][0][
        0
    ]["value"]
    for body in (
        "MATCH (a)-[:R]->(b)",
        "MATCH (a)-[:R]->(b) RETURN b",
        "MATCH (a)-[:R]->(b) WITH b WHERE b.score>=0",
    ):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (a:D) WHERE antfly.documentKey(a)=$key RETURN EXISTS {{ {body} }}",
                "parameters": {"key": key},
                "limits": {"explored_edges": 1},
            },
        )
        assert result["rows"] == [[{"type": "boolean", "value": True}]]
        assert result["stats"]["explored_edges"] == 1


def test_lower_bound_shortest_keeps_longer_qualifying_dag_routes(graph_db):
    db = graph_db
    db.cypher_json(
        "g",
        {
            "query": "CREATE (a:D {id:'a'}),(b:D),(c:D {id:'c'}),(d:D),(e:D),(a)-[:R]->(b),(a)-[:R]->(d),(d)-[:R]->(b),(b)-[:R]->(e),(e)-[:R]->(c)"
        },
    )
    for lower, expected in ((2, 3), (4, 4)):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH p=allShortestPaths((a:D {{id:'a'}})-[:R*{lower}..8]->(c:D {{id:'c'}})) RETURN length(p)",
                "limits": {"explored_edges": 5},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]


def test_unused_wide_relational_columns_are_not_hydrated(graph_db):
    db = graph_db
    db.sql("CREATE TABLE wide (score BIGINT,payload TEXT)")
    for start in range(0, 1000, 100):
        db.sql(
            "INSERT INTO wide (_id,score,payload) VALUES "
            + ",".join(f"('n{i}',{i},'{('x' * 8192)}')" for i in range(start, start + 100))
        )
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "wide",
            "definition": {"labels": [{"name": "W", "table": "wide"}], "relationships": []},
        }
    )
    for forwarding in ("", "WITH n ", "WITH n AS m WITH m AS n ", "WITH * "):
        result = db.cypher_json(
            "wide",
            {"query": f"MATCH (n:W) {forwarding}RETURN count(*),sum(n.score)", "limits": {"memory_bytes": 2 << 20}},
        )
        assert [cell["value"] for cell in result["rows"][0]] == ["1000", "499500"]
        assert result["stats"]["memory_bytes"] < 2 << 20
    entity = db.cypher_json("wide", {"query": "MATCH (n:W) WHERE antfly.documentKey(n)='n0' RETURN n"})
    assert entity["rows"][0][0]["value"]["properties"]["value"]["payload"]["value"] == "x" * 8192
    forwarded = db.cypher_json("wide", {"query": "MATCH (n:W) WHERE antfly.documentKey(n)='n0' WITH n AS m RETURN m"})
    assert forwarded["rows"][0][0] == entity["rows"][0][0]


def test_lower_bound_shortest_reads_only_the_winning_frontier(graph_db):
    db = graph_db
    count = 151
    patterns = [f"(n{i}:D {{id:'n{i}'}})" for i in range(count)]
    patterns += [f"(n{i})-[:R]->(n{i + 1})" for i in range(count - 1)]
    keys = db.cypher_json(
        "g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(n0),antfly.documentKey(n2)"}
    )["rows"][0]
    parameters = {"s": keys[0]["value"], "t": keys[1]["value"]}
    for mode in ("", "REPEATABLE ELEMENTS "):
        for selector in ("shortestPath", "allShortestPaths"):
            result = db.cypher_json(
                "g",
                {
                    "query": "MATCH (s:D) WHERE antfly.documentKey(s)=$s "
                    "MATCH (t:D) WHERE antfly.documentKey(t)=$t "
                    f"MATCH {mode}p={selector}((s)-[:R*2..200]->(t)) RETURN length(p)",
                    "parameters": parameters,
                    "limits": {"scanned_anchors": 2, "explored_edges": 2, "memory_bytes": 256 << 10},
                },
            )
            assert result["rows"] == [[{"type": "integer", "value": "2"}]]
            assert result["stats"]["explored_edges"] == 2
    limited = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D) WHERE antfly.documentKey(s)=$s "
            "MATCH p=shortestPath((s)-[:R*2..200]->(t:D)) RETURN length(p) LIMIT 1",
            "parameters": parameters,
            "limits": {"scanned_anchors": 1, "explored_edges": 2},
        },
    )
    assert limited["rows"] == [[{"type": "integer", "value": "2"}]]


def test_interior_document_key_is_the_physical_anchor(graph_db):
    db = graph_db
    count = 151
    patterns = [f"(n{i}:D {{id:'n{i}'}})" for i in range(count)]
    patterns += [f"(n{i})-[:R]->(n{i + 1})" for i in range(count - 1)]
    key = db.cypher_json("g", {"query": "CREATE " + ",".join(patterns) + " RETURN antfly.documentKey(n75)"})["rows"][0][
        0
    ]["value"]
    for left, right in (("", ""), ("*1..1", "*1..1")):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH p=(a:D)-[left:R{left}]->(m:D)-[right:R{right}]->(b:D) "
                "WHERE antfly.documentKey(m)=$key RETURN [n IN nodes(p)|n.id],a.id,b.id",
                "parameters": {"key": key},
                "limits": {"scanned_anchors": 1, "explored_edges": 2, "memory_bytes": 256 << 10},
            },
        )
        assert [[cell["value"] for cell in row] for row in result["rows"]] == [
            [[{"type": "string", "value": f"n{i}"} for i in (74, 75, 76)], "n74", "n76"]
        ]
        assert result["stats"]["scanned_anchors"] == 1
        assert result["stats"]["explored_edges"] == 2


def test_cyclic_lower_bound_routes_reuse_reads_without_merging_histories(graph_db):
    db = graph_db
    keys = db.cypher_json(
        "g",
        {
            "query": "CREATE (s:D),(u:D),(t:D),(s)-[:R]->(u),(s)-[:R]->(u),"
            "(u)-[:R]->(s),(u)-[:R]->(t) RETURN antfly.documentKey(s),antfly.documentKey(t)"
        },
    )["rows"][0]
    for mode, lower, expected in (("", 2, 2), ("", 3, 2), ("", 6, 0), ("REPEATABLE ELEMENTS ", 6, 8)):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (s:D) WHERE antfly.documentKey(s)=$s "
                "MATCH (t:D) WHERE antfly.documentKey(t)=$t "
                f"MATCH {mode}p=allShortestPaths((s)-[:R*{lower}..8]->(t)) RETURN count(*)",
                "parameters": {"s": keys[0]["value"], "t": keys[1]["value"]},
                "limits": {"explored_edges": 4, "intermediate_states": 2000, "memory_bytes": 1 << 20},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
        assert result["stats"]["explored_edges"] == 4


def test_pattern_equality_joins_use_indexes_or_bounded_hash_lookups(graph_db):
    db = graph_db
    populate(db, 150)
    db.sql("INSERT INTO docs (_id,score) VALUES ('duplicate',1),('null',NULL)")
    for indexed in (False, True):
        if indexed:
            db.sql("CREATE INDEX docs_score ON docs(score)")
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D),(b:D) WHERE a.score=b.score RETURN count(*)",
                "limits": {"scanned_anchors": 500, "memory_bytes": 4 << 20},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": "153"}]]
        assert result["stats"]["scanned_anchors"] <= 500
        # Join execution is independent of downstream fusion eligibility.
        for tail in (
            "UNWIND [1] AS x RETURN count(*)",
            "WITH collect(a.score) AS scores RETURN size(scores)",
        ):
            materialized = db.cypher_json(
                "g",
                {
                    "query": "MATCH (a:D),(b:D) WHERE a.score=b.score " + tail,
                    "limits": {"scanned_anchors": 500, "memory_bytes": 4 << 20},
                },
            )
            assert materialized["rows"] == [[{"type": "integer", "value": "153"}]]
            assert materialized["stats"]["scanned_anchors"] <= 500
        for match in (
            "MATCH (a:D) MATCH (b:D)",
            "OPTIONAL MATCH (a:D),(b:D)",
            "MATCH (a:D) OPTIONAL MATCH (b:D)",
        ):
            joined = db.cypher_json(
                "g",
                {
                    "query": match + " WHERE a.score=b.score RETURN count(b)",
                    "limits": {"scanned_anchors": 500, "memory_bytes": 4 << 20},
                },
            )
            assert joined["rows"] == [[{"type": "integer", "value": "153"}]]
            assert joined["stats"]["scanned_anchors"] <= 500
        projected = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D),(b:D) WHERE a.score=b.score RETURN a.score,b.score ORDER BY a.score,b.score",
                "limits": {"scanned_anchors": 500, "memory_bytes": 4 << 20},
            },
        )
        assert len(projected["rows"]) == 153
        assert all(row[0] == row[1] for row in projected["rows"])
        assert projected["stats"]["scanned_anchors"] <= 500


def test_distinct_streams_unique_keys_and_propagates_limit_demand(graph_db):
    db = graph_db
    populate(db, 2000)
    for query, expected in (
        ("MATCH (n:D) RETURN count(DISTINCT n.score%2)", [["2"]]),
        ("MATCH (n:D) RETURN min(DISTINCT n.score),max(DISTINCT n.score)", [["0", "1999"]]),
        ("MATCH (n:D) RETURN sum(DISTINCT n.score%2),avg(DISTINCT n.score%2)", [["1", 0.5]]),
        ("MATCH (n:D) RETURN DISTINCT n.score%2 AS bucket ORDER BY bucket", [["0"], ["1"]]),
        ("MATCH (n:D) WITH DISTINCT n.score%2 AS bucket RETURN count(*)", [["2"]]),
        ("MATCH (n:D) WITH DISTINCT n.score%2 AS bucket RETURN bucket ORDER BY bucket", [["0"], ["1"]]),
    ):
        result = db.cypher_json("g", {"query": query, "limits": {"memory_bytes": 2 << 20}})
        assert [[cell["value"] for cell in row] for row in result["rows"]] == expected
    limited = db.cypher_json(
        "g",
        {
            "query": "MATCH (n:D) RETURN DISTINCT n.score%2 AS bucket LIMIT 1",
            "limits": {"scanned_anchors": 1, "memory_bytes": 2 << 20},
        },
    )
    assert limited["rows"] == [[{"type": "integer", "value": "0"}]]
    assert limited["stats"]["scanned_anchors"] == 1


def test_sql_graph_table_prunes_columns_pushes_filters_and_pulls_rows(graph_db):
    db = graph_db
    db.sql("CREATE TABLE wide (score BIGINT,payload TEXT)")
    payload = "x" * 32768
    for start in range(0, 1000, 50):
        db.sql(
            "INSERT INTO wide (_id,score,payload) VALUES "
            + ",".join(f"('n{i}',{i},'{payload}')" for i in range(start, start + 50))
        )
    db.sql("CREATE INDEX wide_score ON wide(score)")
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "wide",
            "definition": {"labels": [{"name": "W", "table": "wide"}], "relationships": []},
        }
    )
    relation = "GRAPH_TABLE(wide MATCH (n IS W) COLUMNS(n.score AS score,n.payload AS unused)) g"
    assert db.sql(f"SELECT g.score FROM {relation} WHERE g.score=999")["rows"] == [["999"]]
    # Observable virtual IDs retain source ordinals after SQL filtering.
    assert db.sql(f"SELECT g._id,g.score FROM {relation} WHERE g.score=999")["rows"] == [["999", "999"]]
    assert db.sql(f"SELECT g.score FROM {relation} WHERE g.score=999 AND g._id='999'")["rows"] == [["999"]]
    assert db.sql(f"SELECT g.score FROM {relation} LIMIT 1")["rows"] == [["0"]]
    assert db.sql(f"SELECT count(*) FROM {relation}")["rows"] == [["1000"]]
    assert db.sql(f"SELECT g.unused FROM {relation} WHERE g.score=999")["rows"] == [[payload]]
    for predicate in ("g.score IN (999)", "g.score IN (998,999,999,NULL)", "g.score=998 OR g.score=999"):
        expected = [["999", payload]] if predicate == "g.score IN (999)" else [["998", payload], ["999", payload]]
        assert db.sql(f"SELECT g.score,g.unused FROM {relation} WHERE {predicate} ORDER BY g.score")["rows"] == expected
    for source, field in (
        (f"(SELECT g.score,g.unused FROM {relation}) x", "score"),
        (
            f"(SELECT y.s AS renamed,y.p AS unused FROM (SELECT g.score AS s,g.unused AS p FROM {relation}) y) x",
            "renamed",
        ),
    ):
        assert db.sql(f"SELECT x.unused FROM {source} WHERE x.{field}=999")["rows"] == [[payload]]
        assert db.sql(f"SELECT x.unused FROM {source} WHERE x.{field} IN (999,NULL)")["rows"] == [[payload]]
    for value in (999, 2):
        assert db.sql(f"SELECT g.score,g.unused FROM {relation} WHERE g.score=$1", [value])["rows"] == [
            [str(value), payload]
        ]
    assert db.sql(f"SELECT x.score FROM (SELECT g.score FROM {relation} LIMIT 1) x WHERE x.score=999")["rows"] == []
    assert db.sql(f"SELECT count(*) FROM {relation} WHERE g.score=999.5")["rows"] == [["0"]]
    assert (
        db.sql(
            f"SELECT g.score FROM {relation} WHERE g.score<0 AND g.score=CAST(CAST($1 AS TEXT) AS BIGINT)", ["invalid"]
        )["rows"]
        == []
    )


def test_sql_graph_predicates_cross_safe_joins_and_short_circuit_null(graph_db):
    db = graph_db
    db.sql("CREATE TABLE wide (score BIGINT,payload TEXT)")
    payload = "x" * 32768
    for start in range(0, 1000, 50):
        db.sql(
            "INSERT INTO wide (_id,score,payload) VALUES "
            + ",".join(f"('n{i}',{i},'{payload}')" for i in range(start, start + 50))
        )
    db.sql("CREATE INDEX wide_score ON wide(score)")
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "wide",
            "definition": {"labels": [{"name": "W", "table": "wide"}], "relationships": []},
        }
    )
    graph = "GRAPH_TABLE(wide MATCH (n IS W) COLUMNS(n.score AS score,n.payload AS payload)) g"
    for source in (
        f"{graph} CROSS JOIN (SELECT 1 AS x) v",
        f"(SELECT 1 AS x) v CROSS JOIN {graph}",
        f"{graph} JOIN (SELECT 1 AS x) v ON TRUE",
        f"{graph} LEFT JOIN (SELECT 1 AS x) v ON FALSE",
        f"(SELECT 1 AS x) v RIGHT JOIN {graph} ON FALSE",
    ):
        assert db.sql(f"SELECT g.payload FROM {source} WHERE g.score=$1", [999])["rows"] == [[payload]]
        # Exercise the binding-time path through an inner query as well.
        assert db.sql(f"SELECT x.payload FROM (SELECT g.payload FROM {source} WHERE g.score=$1) x", [999])["rows"] == [
            [payload]
        ]
    for op in ("=", "<>", "<", "<=", ">", ">="):
        assert db.sql(f"SELECT g.payload FROM {graph} WHERE g.score{op}$1", [None])["rows"] == []
    # Mutation captures use the same predicate routing before their read cut.
    db.sql("CREATE TABLE picked (score BIGINT,payload TEXT)")
    db.sql(
        f"INSERT INTO picked (score,payload) SELECT g.score,g.payload FROM {graph} CROSS JOIN (SELECT 1 AS x) v WHERE g.score=999"
    )
    assert db.sql("SELECT score FROM picked")["rows"] == [["999"]]
    # Repeated executions must not contaminate subsequent parameter bindings.
    assert db.sql(f"SELECT g.payload FROM {graph} WHERE g.score=$1", [999])["rows"] == [[payload]]


def test_sql_graph_pushdown_quota_preserves_residual_predicates(graph_db):
    db = graph_db
    populate(db, 2000)

    def balanced(terms):
        if len(terms) == 1:
            return terms[0]
        middle = len(terms) // 2
        return f"({balanced(terms[:middle])} AND {balanced(terms[middle:])})"

    graph = "GRAPH_TABLE(g MATCH (n IS D) COLUMNS(n.score AS score)) g"
    for last, count in (("g.score>=0", "2000"), ("g.score<0", "0")):
        predicate = balanced(["g.score>=0"] * 256 + [last])
        assert db.sql(f"SELECT count(*) FROM {graph} WHERE {predicate}")["rows"] == [[count]]


def test_sql_graph_pushdown_preserves_null_extension_and_query_barriers(graph_db):
    db = graph_db
    populate(db, 3)
    graph = "GRAPH_TABLE(g MATCH (n IS D) COLUMNS(n.score AS score)) g"
    # Pushing the WHERE into the nullable side can manufacture unmatched rows.
    assert db.sql(
        f"SELECT count(*) FROM (SELECT 1 AS x) v LEFT JOIN {graph} ON TRUE WHERE g.score=999 OR g.score IS NULL"
    )["rows"] == [["0"]]
    assert db.sql(
        f"SELECT count(*) FROM {graph} RIGHT JOIN (SELECT 1 AS x) v ON TRUE WHERE g.score=999 OR g.score IS NULL"
    )["rows"] == [["0"]]
    assert db.sql(
        f"SELECT count(*) FROM {graph} FULL JOIN (SELECT 1 AS x) v ON TRUE WHERE g.score=999 OR g.score IS NULL"
    )["rows"] == [["0"]]
    assert db.sql(
        f"SELECT count(*) FROM (SELECT g.score FROM {graph} LIMIT 1) q CROSS JOIN (SELECT 1 AS x) v WHERE q.score=2"
    )["rows"] == [["0"]]


def test_collect_retains_values_instead_of_source_rows(graph_db):
    db = graph_db
    populate(db, 2000)
    db.sql("INSERT INTO docs (_id,score) VALUES ('null',NULL)")
    for distinct, size in (("", 2000), ("DISTINCT ", 2)):
        result = db.cypher_json(
            "g",
            {"query": f"MATCH (n:D) RETURN collect({distinct}n.score%2)", "limits": {"memory_bytes": 2 << 20}},
        )
        values = result["rows"][0][0]["value"]
        assert len(values) == size
        assert {value["value"] for value in values} == {"0", "1"}
        assert result["stats"]["scanned_anchors"] == 2001
        assert result["stats"]["memory_bytes"] < 2 << 20
    result = db.cypher_json(
        "g",
        {
            "query": "MATCH (n:D) WITH DISTINCT n.score%2 AS bucket RETURN collect(bucket)",
            "limits": {"memory_bytes": 2 << 20},
        },
    )
    assert {value["value"] for value in result["rows"][0][0]["value"]} == {"0", "1"}


def test_sql_path_cursor_pulls_traversals_and_respects_residual_limit(graph_db):
    db = graph_db
    count = 18
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(count)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(count) for j in range(i + 1, count)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    # There are 31,824 six-hop routes. A LIMIT 1 consumer must not retain them.
    pattern = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    query = f"SELECT q.score FROM GRAPH_TABLE(g MATCH {pattern} COLUMNS(b.score AS score)) q LIMIT 1"
    # A bounded first pull should not depend on how many routes remain.
    import time

    start = time.monotonic()
    assert len(db.sql(query)["rows"]) == 1
    assert time.monotonic() - start < 5
    onehop = "GRAPH_TABLE(g MATCH (a IS D)-[r IS R]->(b IS D) COLUMNS(a.score AS source,b.score AS target,r AS edge)) q"
    rows = []
    with closing(db.sql_cursor(f"SELECT q.source,q.target FROM {onehop}")) as cursor:
        while True:
            page = cursor.fetch(17)
            rows.extend(page["result"]["rows"])
            if not page["result"]["rows"]:
                break
    assert len(rows) == 153
    assert {(int(source), int(target)) for source, target in rows} == {
        (i, j) for i in range(count) for j in range(i + 1, count)
    }
    # Residual predicates need additional pulls; row_goal cannot truncate input.
    assert db.sql(f"SELECT q.source,q.target FROM {onehop} WHERE q.target+0=17 AND q.source+0=16 LIMIT 1")["rows"] == [
        ["16", "17"]
    ]
    assert db.sql(f"SELECT count(*) FROM {onehop}")["rows"] == [["153"]]


def test_sql_graph_boolean_filters_keep_three_valued_semantics_and_barriers(graph_db):
    db = graph_db
    populate(db, 3)
    db.sql("INSERT INTO docs (_id,score) VALUES ('null',NULL)")
    relation = "GRAPH_TABLE(g MATCH (n IS D) COLUMNS(n.score AS score)) g"
    for predicate, expected in (
        ("g.score IN (1,1,NULL)", [["1"]]),
        ("g.score NOT IN (0,NULL)", []),
        ("g.score=0 OR CAST(g.score AS TEXT)='1'", [["0"], ["1"]]),
        ("g.score=0 OR g.score IS NULL", [["0"], [None]]),
        ("(g.score=0 OR g.score=1) AND g.score>0", [["1"]]),
    ):
        assert (
            db.sql(f"SELECT g.score FROM {relation} WHERE {predicate} ORDER BY g.score NULLS LAST")["rows"] == expected
        )
    # Compare bound literal coercion against the same relation with pushdown blocked.
    predicate = "g.score=0 OR g.score=1.5"
    baseline = f"(SELECT g.score FROM {relation} OFFSET 0) g"
    assert (
        db.sql(f"SELECT g.score FROM {relation} WHERE {predicate} ORDER BY g.score")["rows"]
        == db.sql(f"SELECT g.score FROM {baseline} WHERE {predicate} ORDER BY g.score")["rows"]
    )
    for value, expected in ((None, []), (1, [["1"]]), (2, [["2"]])):
        assert db.sql(f"SELECT g.score FROM {relation} WHERE g.score IN ($1,NULL)", [value])["rows"] == expected
    assert (
        db.sql(f"SELECT x.score FROM (SELECT g.score FROM {relation} ORDER BY g.score LIMIT 1) x WHERE x.score=2")[
            "rows"
        ]
        == []
    )
    grouped = f"(SELECT g.score%2 AS bucket,count(*) AS total FROM {relation} GROUP BY g.score%2) x"
    assert db.sql(f"SELECT x.total FROM {grouped} WHERE x.bucket=0")["rows"] == [["2"]]


def test_native_or_and_required_label_predicates_seek_the_index(graph_db):
    db = graph_db
    populate(db, 3000)
    db.sql("CREATE INDEX docs_score ON docs(score)")
    for predicate in (
        "n.score=2998 OR n.score=2999",
        "n.score=2998 OR n.score IN [2999,null]",
        "2998=n.score OR n.score=2999",
    ):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (n:D) WHERE {predicate} RETURN n.score ORDER BY n.score",
                "limits": {"scanned_anchors": 2},
            },
        )
        assert [row[0]["value"] for row in result["rows"]] == ["2998", "2999"]
        assert result["stats"]["scanned_anchors"] == 2
    # A non-membership arm must decline the whole OR hint.
    mixed = db.cypher_json(
        "g", {"query": "MATCH (n:D) WHERE n.score=2999 OR n.id='n0' RETURN n.score ORDER BY n.score"}
    )
    assert [row[0]["value"] for row in mixed["rows"]] == ["0", "2999"]
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "rare",
            "definition": {
                "labels": [
                    {"name": "Rare", "table": "docs", "where": {"score": 2999}},
                    {"name": "Common", "table": "docs", "where": {"score": 0}},
                ],
                "relationships": [],
            },
        }
    )
    rare = db.cypher_json("rare", {"query": "MATCH (n:Rare) RETURN count(*)", "limits": {"scanned_anchors": 1}})
    assert rare["rows"] == [[{"type": "integer", "value": "1"}]]
    assert rare["stats"]["scanned_anchors"] == 1
    union = db.cypher_json("rare", {"query": "MATCH (n) RETURN n.score ORDER BY n.score"})
    assert [row[0]["value"] for row in union["rows"]] == ["0", "2999"]
    assert db.cypher_json("rare", {"query": "MATCH (n:Rare:Common) RETURN count(*)"})["rows"] == [
        [{"type": "integer", "value": "0"}]
    ]


def test_unwind_fuses_with_reducers_and_carries_limit_demand(graph_db):
    db = graph_db
    populate(db, 3000)
    result = db.cypher_json(
        "g", {"query": "MATCH (n:D) UNWIND [1] AS x RETURN count(*)", "limits": {"memory_bytes": 2 << 20}}
    )
    assert result["rows"] == [[{"type": "integer", "value": "3000"}]]
    limited = db.cypher_json(
        "g",
        {"query": "MATCH (n:D) UNWIND [1,2] AS x RETURN n.score,x LIMIT 1", "limits": {"scanned_anchors": 1}},
    )
    assert limited["rows"] == [[{"type": "integer", "value": "0"}, {"type": "integer", "value": "1"}]]
    assert limited["stats"]["scanned_anchors"] == 1


def test_sql_comma_patterns_suspend_the_whole_join_at_limit(graph_db):
    db = graph_db
    populate(db, 3000)
    relation = "GRAPH_TABLE(g MATCH (a IS D),(b IS D) COLUMNS(a.score AS first,b.score AS second)) q"
    assert db.sql(f"SELECT q.first,q.second FROM {relation} LIMIT 1")["rows"] == [["0", "0"]]
    # The residual needs several pulls, so demand must never become an inner LIMIT.
    assert db.sql(f"SELECT q.first,q.second FROM {relation} WHERE q.second+0=7 LIMIT 1")["rows"] == [["0", "7"]]
    db.sql("CREATE INDEX docs_score ON docs(score)")
    joined = "GRAPH_TABLE(g MATCH (a IS D),(b IS D) WHERE a.score=b.score COLUMNS(a.score AS score)) q"
    assert db.sql(f"SELECT q.score FROM {joined} LIMIT 1")["rows"] == [["0"]]


def test_all_paths_count_uses_dag_states_for_native_and_sql(graph_db):
    from math import comb

    db = graph_db
    size = 18
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    expected = comb(size, 7)
    fixed = "(a:D)" + "".join(f"-[:R]->(m{i}:D)" for i in range(5)) + "-[:R]->(b:D)"
    for pattern in (fixed, "(a:D)-[:R*6..6]->(b:D)"):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH {pattern} RETURN count(*)",
                "limits": {"explored_edges": 1000, "intermediate_states": 20000, "memory_bytes": 4 << 20},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
        assert result["stats"]["explored_edges"] <= 1000
    for tail in (
        "RETURN count(b)",
        "WHERE b.score>=0 RETURN count(*)",
        "RETURN b.score,count(b),count(b.missing),count(*) ORDER BY b.score",
    ):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D)-[:R*6..6]->(b:D) " + tail,
                "limits": {"explored_edges": 1000, "intermediate_states": 25000, "memory_bytes": 4 << 20},
            },
        )
        if tail.startswith("RETURN b.score"):
            assert [[cell["value"] for cell in row] for row in result["rows"]] == [
                [str(endpoint), str(comb(endpoint, 6)), "0", str(comb(endpoint, 6))] for endpoint in range(6, size)
            ]
        else:
            assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
        assert result["stats"]["explored_edges"] <= 1000
    grouped = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*6..6]->(b:D) RETURN b.score AS endpoint,count(*) AS total ORDER BY endpoint",
            "limits": {"explored_edges": 1000, "intermediate_states": 30000},
        },
    )
    assert [[cell["value"] for cell in row] for row in grouped["rows"]] == [
        [str(endpoint), str(comb(endpoint, 6))] for endpoint in range(6, size)
    ]
    reduced = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*6..6]->(b:D) RETURN count(*),sum(b.score),min(b.score),max(b.score),avg(b.score),count(DISTINCT b)",
            "limits": {"explored_edges": 1000, "intermediate_states": 30000, "memory_bytes": 4 << 20},
        },
    )
    total = sum(endpoint * comb(endpoint, 6) for endpoint in range(6, size))
    assert [cell["value"] for cell in reduced["rows"][0]] == [
        str(expected),
        str(total),
        "6",
        "17",
        pytest.approx(total / expected),
        "12",
    ]
    rejected = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D {id:'n0'})-[:R]->(m:D {score:999999})-[:R]->(b:D) RETURN count(*)",
            "limits": {"explored_edges": size - 1},
        },
    )
    assert rejected["rows"] == [[{"type": "integer", "value": "0"}]]
    assert rejected["stats"]["explored_edges"] == size - 1
    sql_pattern = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    # Exercise the public SQL exact-count hook, including both cursor adapters.
    # Enumerating 31,824 rows took over 20 seconds before count delegation.
    import time

    start = time.monotonic()
    assert db.sql(f"SELECT count(*) FROM GRAPH_TABLE(g MATCH {sql_pattern} COLUMNS(b.score AS score)) q")["rows"] == [
        [str(expected)]
    ]
    assert time.monotonic() - start < 5


def test_all_path_product_states_preserve_endpoint_constraints_and_bags(graph_db):
    db = graph_db
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(6)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(6) for j in range(i + 1, 6)]
    # Parallel edges must contribute separate ways through every product state.
    patterns += ["(n0)-[:R]->(n1)"]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for pattern in (
        "(a:D)-[:R*0..3]->(b:D)",
        "(a:D)-[:R*0..1]->(b:D)-[:R*0..2]->(c:D)",
        "(a:D)-[:R]->(b:D {score:2})-[:R*1..3]->(c:D)",
        "(a:D)-[r:R]->(b:D)-[r:R]->(c:D)",
        "(a:D)-[:R*1..3]->(a:D)",
        "(a:D)-[:R]-(b:D)-[:R]->(c:D)",
    ):
        optimized = db.cypher_json("g", {"query": f"MATCH {pattern} RETURN count(*)"})
        enumerated = db.cypher_json("g", {"query": f"MATCH {pattern} UNWIND [1] AS x RETURN count(*)"})
        assert optimized["rows"] == enumerated["rows"], pattern
    relation = "GRAPH_TABLE(g MATCH (a IS D)-[r IS R]->(b IS D) COLUMNS(b.score AS score)) q"
    assert db.sql(f"SELECT count(*) FROM {relation} WHERE q.score+0=5")["rows"] == [["5"]]
    assert db.sql(f"SELECT count(*) FROM (SELECT q.score FROM {relation} LIMIT 3) limited")["rows"] == [["3"]]


def test_indexed_bulk_merge_has_linear_staged_work(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    previous = 0
    for count in (200, 400, 800):
        result = db.cypher_json(
            "g",
            {
                "query": "UNWIND $ids AS id MERGE (n:D {id:id}) RETURN count(*)",
                "parameters": {"ids": [f"batch{count}_n{i}" for i in range(count)]},
                "limits": {"intermediate_states": 200000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(count)}]]
        assert result["stats"]["scanned_anchors"] == 0
        work = result["stats"]["intermediate_states"]
        if previous:
            assert work < previous * 3
        previous = work
    repeated = db.cypher_json(
        "g", {"query": "UNWIND $ids AS id MERGE (n:D {id:id}) RETURN count(*)", "parameters": {"ids": ["same"] * 800}}
    )
    assert repeated["rows"] == [[{"type": "integer", "value": "800"}]]
    assert repeated["stats"]["scanned_anchors"] == 799
    assert db.cypher_json("g", {"query": "MATCH (n:D {id:'same'}) RETURN count(*)"})["rows"] == [
        [{"type": "integer", "value": "1"}]
    ]


def test_foldable_negative_predicates_seek_indexes(graph_db):
    db = graph_db
    populate(db, 1000)
    db.sql("UPDATE docs SET score=-score-1")
    db.sql("CREATE INDEX docs_score ON docs(score)")
    for expression in ("-1", "1-2", "-$positive", "$zero-1"):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (n:D) WHERE n.score={expression} RETURN n.score",
                "parameters": {"positive": 1, "zero": 0},
                "limits": {"scanned_anchors": 1},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": "-1"}]]
        assert result["stats"]["scanned_anchors"] == 1


def test_sql_interior_anchor_stops_at_outer_demand(graph_db):
    import time

    db = graph_db
    db.sql("CREATE INDEX docs_score ON docs(score)")
    db.cypher_json("g", {"query": "CREATE (:D {id:'center',score:0})"})
    for start in range(0, 200, 25):
        patterns = []
        for index in range(start, start + 25):
            patterns += [
                f"(l{index}:D {{id:'l{index}',score:1}})",
                f"(r{index}:D {{id:'r{index}',score:2}})",
                f"(l{index})-[:R]->(m)",
                f"(m)-[:R]->(r{index})",
            ]
        db.cypher_json("g", {"query": "MATCH (m:D {score:0}) CREATE " + ",".join(patterns)})
    relation = (
        "GRAPH_TABLE(g MATCH (a IS D)-[x IS R]->(m IS D)-[y IS R]->(b IS D) "
        "WHERE m.score=0 COLUMNS(a.id AS a,b.id AS b)) q"
    )
    start = time.monotonic()
    rows = db.sql(f"SELECT q.a,q.b FROM {relation} LIMIT 1")["rows"]
    assert len(rows) == 1
    assert rows[0][0].startswith("l") and rows[0][1].startswith("r")
    assert time.monotonic() - start < 5


def test_relationship_merge_indexes_bound_pairs_and_reuses_native_adjacency(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    db.cypher_json("g", {"query": "CREATE (:D {id:'root'})"})
    query = "MATCH (a:D {id:'root'}) UNWIND $ids AS id MERGE (b:D {id:id}) MERGE (a)-[:R]->(b) RETURN count(*)"
    result = db.cypher_json("g", {"query": query, "parameters": {"ids": [f"n{i}" for i in range(800)]}})
    assert result["rows"] == [[{"type": "integer", "value": "800"}]]
    assert result["stats"]["explored_edges"] == 0
    assert result["stats"]["intermediate_states"] < 100000
    repeated = db.cypher_json(
        "g", {"query": query, "parameters": {"ids": [f"n{i}" for i in range(200)]}, "limits": {"explored_edges": 1200}}
    )
    assert repeated["rows"] == [[{"type": "integer", "value": "200"}]]
    assert repeated["stats"]["explored_edges"] <= 1200
    assert db.cypher_json("g", {"query": "MATCH ()-[:R]->() RETURN count(*)"})["rows"] == [
        [{"type": "integer", "value": "800"}]
    ]
