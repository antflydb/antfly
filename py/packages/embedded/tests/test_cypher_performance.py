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
    start = time.monotonic()
    aggregate = db.sql(
        f"SELECT sum(q.score),min(q.score),max(q.score),sum(DISTINCT q.score) "
        f"FROM GRAPH_TABLE(g MATCH {sql_pattern} COLUMNS(b.score AS score)) q"
    )
    assert [int(cell) for cell in aggregate["rows"][0]] == [total, 6, 17, sum(range(6, size))]
    assert time.monotonic() - start < 5
    # Pure scalar programs preserve multiplicities, including lazy branches,
    # Boolean conditions and builtin calls that cannot use parallel execution.
    start = time.monotonic()
    calculated = db.sql(
        f"SELECT sum(CASE WHEN q.score>0 THEN q.score ELSE 1/(q.score-q.score) END),"
        f"min(abs(q.score)),max(coalesce(q.score,0)),sum(DISTINCT abs(q.score)) "
        f"FROM GRAPH_TABLE(g MATCH {sql_pattern} COLUMNS(b.score AS score)) q "
        f"WHERE q.score IN (6,7,8,9,10,11,12,13,14,15,16,17) AND (q.score>0 OR q.score IS NULL)"
    )
    assert [int(cell) for cell in calculated["rows"][0]] == [total, 6, 17, sum(range(6, size))]
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


def test_mixed_and_unbounded_dag_domains_keep_physical_work_bounded(graph_db):
    from math import comb

    db = graph_db
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "mixed",
            "definition": {
                "labels": [{"name": "D", "table": "docs"}],
                "relationships": [
                    {
                        "name": name,
                        "index": "links",
                        "source_label": "D",
                        "target_label": "D",
                        "stored_type": name.lower(),
                    }
                    for name in ("R", "S")
                ],
            },
        }
    )
    size = 18
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:{kind}]->(n{j})" for kind in ("R", "S") for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("mixed", {"query": "CREATE " + ",".join(patterns)})
    for pattern, expected in (
        ("(a:D)-[:R*3..3]->()-[:S*3..3]->(b:D)", comb(size, 7)),
        ("(a:D)-[:R*6..]->(b:D)", sum(comb(size, depth + 1) for depth in range(6, size))),
    ):
        result = db.cypher_json(
            "mixed",
            {
                "query": f"MATCH {pattern} RETURN count(*)",
                "limits": {"explored_edges": 2000, "intermediate_states": 40000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
        assert result["stats"]["explored_edges"] < 2000


def test_bound_endpoint_read_and_count_seek_native_hub_pairs(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    db.cypher_json("g", {"query": "CREATE (:D {id:'root'})"})
    db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D {id:'root'}) UNWIND $ids AS id CREATE (b:D {id:id}) CREATE (a)-[:R]->(b)",
            "parameters": {"ids": [f"n{i}" for i in range(800)]},
        },
    )
    prefix = "MATCH (a:D {id:'root'}),(b:D {id:'n799'}) MATCH "
    for pattern in ("(a)-[:R]->(b)", "(b)<-[:R]-(a)"):
        for projection, expected in (
            ("count(*)", [[{"type": "integer", "value": "1"}]]),
            ("a.id LIMIT 1", [[{"type": "string", "value": "root"}]]),
        ):
            result = db.cypher_json(
                "g", {"query": prefix + pattern + " RETURN " + projection, "limits": {"explored_edges": 10}}
            )
            assert result["rows"] == expected
            assert result["stats"]["explored_edges"] <= 2


def test_disjoint_direction_domains_reduce_trails_without_reusing_relationships(graph_db):
    from math import comb

    db = graph_db
    db.graph_catalog_json(
        {
            "action": "put",
            "name": "disjoint",
            "definition": {
                "labels": [{"name": "D", "table": "docs"}],
                "relationships": [
                    {
                        "name": kind,
                        "index": "links",
                        "source_label": "D",
                        "target_label": "D",
                        "stored_type": kind.lower(),
                    }
                    for kind in ("R", "S")
                ],
            },
        }
    )
    size = 16
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:{kind}]->(n{j})" for kind in ("R", "S") for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("disjoint", {"query": "CREATE " + ",".join(patterns)})
    expected = sum(comb(peak, 3) ** 2 for peak in range(3, size))
    for mode in ("", "REPEATABLE ELEMENTS "):
        result = db.cypher_json(
            "disjoint",
            {
                "query": f"MATCH {mode}(a:D)-[:R*3..3]->()<-[:S*3..3]-(b:D) RETURN count(*)",
                "limits": {"intermediate_states": 30000, "explored_edges": 2500, "memory_bytes": 4 << 20},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
    # A mutation is a clause barrier, not a reason to enumerate the following
    # read phase's route bag. All preceding effects must still be committed.
    written = db.cypher_json(
        "disjoint",
        {
            "query": "CREATE (:D {id:'marker'}) WITH 1 AS ignored MATCH (a:D)-[:R*6..6]->(b:D) RETURN count(*)",
            "limits": {"intermediate_states": 30000, "explored_edges": 3000, "memory_bytes": 4 << 20},
        },
    )
    assert written["rows"] == [[{"type": "integer", "value": str(comb(size, 7))}]]
    assert db.sql("SELECT count(*) FROM docs WHERE id='marker'")["rows"] == [["1"]]


def test_write_read_phases_pull_parallel_pair_pages_and_preserve_postimages(graph_db):
    db = graph_db
    first, second = [
        cell["value"]
        for cell in db.cypher_json(
            "g",
            {
                "query": "CREATE (a:D {id:'a',score:1}),(b:D {id:'b',score:2}) "
                "RETURN antfly.documentKey(a),antfly.documentKey(b)"
            },
        )["rows"][0]
    ]
    prefix = "MATCH (a:D),(b:D) WHERE antfly.documentKey(a)=$first AND antfly.documentKey(b)=$second "
    params = {"first": first, "second": second}
    db.cypher_json(
        "g",
        {
            "query": prefix + "UNWIND range(0,999) AS slot CREATE (a)-[:R {slot:slot}]->(b)",
            "parameters": params,
        },
    )
    result = db.cypher_json(
        "g",
        {
            "query": prefix + "CREATE (:D {id:'marker'}) WITH a,b MATCH (a)-[:R]->(b) RETURN a.id LIMIT 1",
            "parameters": params,
            "limits": {"explored_edges": 10, "intermediate_states": 5000, "memory_bytes": 1 << 20},
        },
    )
    assert result["rows"] == [[{"type": "string", "value": "a"}]]
    assert result["stats"]["explored_edges"] == 1
    # Read-only phases must refresh aliases after SET, carry all rows into
    # subsequent writes and avoid observing cached preimages across barriers.
    updated = db.cypher_json(
        "g",
        {
            "query": prefix + "SET a.score=9 WITH a,b MATCH (a)-[r:R]->(b) WITH a,b,r LIMIT 2 "
            "SET r.slot=2000 WITH DISTINCT a,b MATCH (a)-[r:R {slot:2000}]->(b) RETURN a.score,count(r)",
            "parameters": params,
        },
    )
    assert updated["rows"] == [[{"type": "integer", "value": "9"}, {"type": "integer", "value": "2"}]]
    refreshed = db.cypher_json(
        "g",
        {
            "query": prefix + "SET a.score=10 WITH a,b MATCH (c:D {score:a.score}) WITH a,b,c LIMIT 1 "
            "SET c.score=11 WITH c MATCH (d:D {score:c.score}) RETURN d.id,d.score",
            "parameters": params,
        },
    )
    assert refreshed["rows"] == [[{"type": "string", "value": "a"}, {"type": "integer", "value": "11"}]]
    reused = db.cypher_json(
        "g",
        {
            "query": prefix + "MATCH (a)-[r:R]->(b) WITH a,b,count(r) AS before "
            "CREATE (a)-[:R {slot:2000}]->(b) WITH a,b,before "
            "MATCH (a)-[r:R {slot:2000}]->(b) RETURN before,count(r)",
            "parameters": params,
            "limits": {"explored_edges": 1100},
        },
    )
    assert reused["rows"] == [[{"type": "integer", "value": "1000"}, {"type": "integer", "value": "3"}]]


def test_presence_reductions_do_not_enumerate_or_require_exact_path_counts(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    layers = 64
    patterns = ["(s:D {id:'start',score:0})"] + [
        f"(v{i}_{j}:D {{score:{i + 1}}})" for i in range(layers) for j in range(2)
    ]
    patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
    patterns += [f"(v{i}_{j})-[:R]->(v{i + 1}_{k})" for i in range(layers - 1) for j in range(2) for k in range(2)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    # There are 2**64 paths but only two endpoints with the same score. Neither
    # DISTINCT nor presence-only SQL reducers need their exact multiplicity.
    for tail, values in (
        ("RETURN DISTINCT b.score", ["64"]),
        ("RETURN DISTINCT b.score ORDER BY b.score DESC LIMIT 1", ["64"]),
        ("RETURN DISTINCT b.score AS b ORDER BY b.score", ["64"]),
        ("RETURN DISTINCT b.score,b.missing ORDER BY b.score", ["64", None]),
        ("WITH DISTINCT b.score AS score RETURN count(*),sum(score)", ["1", "64"]),
        ("WITH DISTINCT b RETURN count(*),sum(b.score)", ["2", "128"]),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) " + tail,
                "limits": {"intermediate_states": 10000, "explored_edges": 300},
            },
        )
        assert [[cell.get("value") for cell in row] for row in result["rows"]] == [values]
        assert result["stats"]["explored_edges"] == 254
        assert result["stats"]["intermediate_states"] < 5000
    optional = db.cypher_json(
        "g",
        {
            "query": "OPTIONAL MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) RETURN min(b.score),count(DISTINCT b)",
            "limits": {"intermediate_states": 10000, "explored_edges": 300},
        },
    )
    assert [cell.get("value") for cell in optional["rows"][0]] == ["64", "2"]
    early = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) RETURN DISTINCT b.score LIMIT 1",
            "limits": {"explored_edges": 64},
        },
    )
    assert early["rows"] == [[{"type": "integer", "value": "64"}]]
    assert early["stats"]["explored_edges"] == 64
    sql_pattern = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(63)) + "-[r63 IS R]->(b IS D)"
    relation = f"GRAPH_TABLE(g MATCH {sql_pattern} WHERE a.id='start' COLUMNS(b.score AS score,b.missing AS missing)) q"
    assert db.sql(
        f"SELECT min(q.score),max(q.score),count(DISTINCT q.score),"
        f"sum(DISTINCT q.score),avg(DISTINCT q.score),"
        f"bool_and(q.score>0),bool_or(q.score<0),count(DISTINCT q.missing) FROM {relation}"
    )["rows"] == [["64", "64", "1", "64", 64, True, False, "0"]]
    assert db.sql(
        f"SELECT q.score,min(q.score),count(DISTINCT q.score) FROM {relation} WHERE q.score IN (64,65) GROUP BY q.score"
    )["rows"] == [["64", "64", "1"]]
    empty = db.sql(f"SELECT min(q.score),count(DISTINCT q.score) FROM {relation} WHERE q.score<0")
    assert empty["rows"] == [[None, "0"]]
    assert empty["sql_nulls"] == [[True, False]]
    assert db.sql(
        f"SELECT sum(DISTINCT CAST(q.score AS DOUBLE PRECISION)),"
        f"avg(DISTINCT CAST(q.score AS DOUBLE PRECISION)) FROM {relation}"
    )["rows"] == [[64, 64]]
    window = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) "
            "WITH b ORDER BY b.score SKIP 100 LIMIT 25 RETURN count(*),sum(b.score)",
            "limits": {"intermediate_states": 10000, "explored_edges": 300},
        },
    )
    assert [cell["value"] for cell in window["rows"][0]] == ["25", "1600"]
    top_relation = f"GRAPH_TABLE(g MATCH {sql_pattern} WHERE a.id='start' COLUMNS(b.score AS score)) q"
    for source in (top_relation, top_relation + " CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 AS k) d"):
        assert db.sql(f"SELECT q.score FROM {source} ORDER BY q.score DESC LIMIT 5 OFFSET 1000")["rows"] == [["64"]] * 5

    # Mixing a presence-only reducer with COUNT requires exact bags. Keep this
    # case below saturation, with more paths than could ever be enumerated.
    relation = relation.replace("-[r62 IS R]->(m62 IS D)-[r63 IS R]->(b IS D)", "")
    relation = relation.replace("(m61 IS D)", "(b IS D)")
    assert db.sql(f"SELECT min(q.score),count(*),sum(q.score-61),sum(CAST(q.score AS NUMERIC)) FROM {relation}")[
        "rows"
    ] == [["62", str(2**62), str(2**62), str(62 * 2**62)]]


def test_pure_endpoint_expressions_keep_compressed_bags(graph_db):
    from math import comb

    db = graph_db
    size = 16
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for expression in (
        "CASE WHEN b.score>0 THEN b.score ELSE 1/0 END",
        "reduce(total=0,x IN [b.score] | total+x)",
        "[x IN [b.score] WHERE x>0 | x][0]",
        "toInteger(toString(b.score))",
    ):
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (a:D)-[:R*6..6]->(b:D) RETURN min({expression})",
                "limits": {"explored_edges": 120, "intermediate_states": 15000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": "6"}]]
    paths = comb(size, 7)
    total = sum(i * comb(i, 6) for i in range(6, size))
    for tail, expected in (
        ("WITH b RETURN count(*),sum(b.score)", [paths, total]),
        ("WITH * RETURN count(*),sum(b.score)", [paths, total]),
        ("WITH b.score AS score RETURN count(*),sum(score)", [paths, total]),
        ("WITH b AS a WITH a.score AS b WHERE b>0 RETURN count(*),sum(b)", [paths, total]),
        ("WITH DISTINCT b RETURN count(*),sum(b.score)", [size - 6, sum(range(6, size))]),
        ("WITH b WHERE b.score>0 RETURN count(*),sum(b.score)", [paths, total]),
        ("WITH b ORDER BY b.score RETURN count(*),min(b.score),max(b.score)", [paths, 6, 15]),
        ("WITH b ORDER BY b.score SKIP 2 LIMIT 5 RETURN count(*),sum(b.score)", [5, 35]),
        ("WITH b ORDER BY b.score DESC SKIP 100 LIMIT 25 RETURN count(*),sum(b.score)", [25, 375]),
        ("WITH b ORDER BY b.missing SKIP 2 LIMIT 5 RETURN count(*)", [5]),
        ("WITH b ORDER BY b.score LIMIT 0 RETURN count(*)", [0]),
        (
            "UNWIND [b.score,b.score,null] AS score RETURN count(*),count(score),sum(score)",
            [3 * paths, 2 * paths, 2 * total],
        ),
        (
            "UNWIND [b.score,b.score] AS score WITH DISTINCT score RETURN count(*),sum(score)",
            [size - 6, sum(range(6, size))],
        ),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D)-[:R*6..6]->(b:D) " + tail,
                "limits": {"explored_edges": 120, "intermediate_states": 10000},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == [str(value) for value in expected]
    for prefix in (
        "MATCH (a:D)-[:R*3..3]->(m:D),(m)-[:R*3..3]->(b:D)",
        "MATCH (a:D)-[:R*3..3]->(m:D) MATCH (m)-[:R*3..3]->(b:D)",
        "MATCH (m:D)-[:R*3..3]->(b:D),(a:D)-[:R*3..3]->(m)",
    ):
        component = db.cypher_json(
            "g",
            {
                "query": prefix + " RETURN count(*),sum(b.score)",
                "limits": {"intermediate_states": 15000, "explored_edges": 120},
            },
        )
        assert [cell["value"] for cell in component["rows"][0]] == [str(paths), str(total)]
    branch_paths = sum(comb(m, 3) * comb(size - m - 1, 3) * (size - m - 2) for m in range(3, size - 3))
    branch_total = sum(
        comb(m, 3) * comb(b - m - 1, 2) * b * (size - m - 2) for m in range(3, size - 3) for b in range(m + 3, size)
    )
    branch = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*3..3]->(m:D),(m)-[:R*3..3]->(b:D),(m)-[:R]->(c:D) RETURN count(*),sum(b.score)",
            "limits": {"intermediate_states": 20000, "explored_edges": 120},
        },
    )
    assert [cell["value"] for cell in branch["rows"][0]] == [str(branch_paths), str(branch_total)]
    for predicate, expected in (
        ("", [paths, paths, total, 6]),
        (" WHERE false", [1, 0, 0, None]),
    ):
        optional = db.cypher_json(
            "g",
            {
                "query": "OPTIONAL MATCH (a:D)-[:R*6..6]->(b:D)"
                + predicate
                + " RETURN count(*),count(b),sum(b.score),min(b.score)",
                "limits": {"intermediate_states": 10000, "explored_edges": 120},
            },
        )
        assert [cell.get("value") for cell in optional["rows"][0]] == [
            str(value) if value is not None else None for value in expected
        ]
    distinct = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*6..6]->(b:D) RETURN DISTINCT b{.score} AS endpoint ORDER BY endpoint",
            "limits": {"explored_edges": 120, "intermediate_states": 15000},
        },
    )
    assert [row[0]["value"]["score"]["value"] for row in distinct["rows"]] == [str(i) for i in range(6, size)]
    path = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    # Moving a pure CASE into GRAPH_TABLE COLUMNS must keep SQL weights.
    import time

    start = time.monotonic()
    result = db.sql(
        f"SELECT min(CAST(q.score AS BIGINT)) FROM GRAPH_TABLE(g MATCH {path} "
        "COLUMNS(CASE WHEN b.score>0 THEN b.score ELSE 1/0 END AS score)) q"
    )
    assert result["rows"] == [["6"]]
    assert time.monotonic() - start < 5
    mixed_relation = f"GRAPH_TABLE(g MATCH {path} COLUMNS(a.score AS s,b.score AS e)) q"
    start_total = sum(i * comb(size - i - 1, 6) for i in range(size - 6))
    mixed = db.sql(
        "SELECT sum(q.s),sum(q.e),count(*),sum(DISTINCT q.s),sum(DISTINCT q.e),"
        "sum(q.s) FILTER (WHERE q.s%2=0),sum(q.e) FILTER (WHERE q.e%2=1) FROM " + mixed_relation
    )
    assert mixed["rows"] == [
        [
            str(start_total),
            str(total),
            str(paths),
            str(sum(range(size - 6))),
            str(sum(range(6, size))),
            str(sum(i * comb(size - i - 1, 6) for i in range(size - 6) if i % 2 == 0)),
            str(sum(i * comb(i, 6) for i in range(6, size) if i % 2 == 1)),
        ]
    ]
    lazy = db.sql("SELECT sum(q.s),sum(q.e),sum(1/(q.s-q.s)) FILTER (WHERE q.s<0) FROM " + mixed_relation)
    assert lazy["rows"] == [[str(start_total), str(total), None]]
    factor = 10**20
    exact = db.sql(
        f"SELECT sum(CAST(q.s AS NUMERIC)*{factor}),sum(CAST(q.e AS NUMERIC)*{factor}) FROM {mixed_relation}"
    )
    assert exact["rows"] == [[str(start_total * factor), str(total * factor)]]
    # A cross-endpoint predicate keeps its exact coupled scan rather than
    # accepting incompatible independent passes.
    assert db.sql(f"SELECT sum(q.s),sum(q.e) FROM {mixed_relation} WHERE q.s<q.e")["rows"] == [
        [str(start_total), str(total)]
    ]

    # Relational aliases, computed projections and predicates must retain
    # weights through derived tables and CTEs, including independent reducers.
    for source, prefix, expected in (
        (f"(SELECT q.e AS score FROM {mixed_relation}) d", "", total),
        (f"(SELECT q.e+1 AS score FROM {mixed_relation}) d", "", total + paths),
        (f"(SELECT x.v AS score FROM (SELECT q.e+1 AS v FROM {mixed_relation}) x) d", "", total + paths),
        ("saved d", f"WITH saved AS (SELECT q.e AS score FROM {mixed_relation}) ", total),
        (f"(SELECT q.e AS score FROM {mixed_relation} WHERE q.e>=6) d", "", total),
    ):
        started = time.monotonic()
        assert db.sql(f"{prefix}SELECT sum(d.score),count(*) FROM {source}")["rows"] == [[str(expected), str(paths)]]
        assert time.monotonic() - started < 5
    assert db.sql(f"SELECT sum(d.s),sum(d.e),count(*) FROM (SELECT q.s+1 AS s,q.e*2 AS e FROM {mixed_relation}) d")[
        "rows"
    ] == [[str(start_total + paths), str(total * 2), str(paths)]]
    assert db.sql(
        f"SELECT sum(d.score) FROM (SELECT CASE WHEN q.e>0 THEN q.e ELSE 1/0 END AS score FROM {mixed_relation}) d"
    )["rows"] == [[str(total)]]
    from antfly_embedded._sql import SQLStateError

    with pytest.raises(SQLStateError, match="22012"):
        db.sql(f"SELECT sum(d.e) FROM (SELECT q.e,1/(q.e-q.e) AS unused FROM {mixed_relation}) d")

    for statement, expected in (
        (f"SELECT sum(d.e),count(*) FROM (SELECT q.e FROM {mixed_relation} ORDER BY q.e) d", [str(total), str(paths)]),
        (
            f"SELECT sum(d.e),count(*) FROM (SELECT q.e FROM {mixed_relation} UNION ALL SELECT q.e FROM {mixed_relation}) d",
            [str(2 * total), str(2 * paths)],
        ),
        (f"SELECT sum(q.e),count(*) FROM {mixed_relation} CROSS JOIN (SELECT 1 AS x) d", [str(total), str(paths)]),
        (
            f"SELECT sum(q.e*d.x),count(*) FROM (SELECT 2 AS x) d CROSS JOIN {mixed_relation}",
            [str(2 * total), str(paths)],
        ),
        (f"SELECT sum(q.e),count(*) FROM {mixed_relation} CROSS JOIN (SELECT 1 AS x WHERE false) d", [None, "0"]),
        (
            f"SELECT sum(d.e),count(DISTINCT d.e) FROM (SELECT q.e FROM {mixed_relation} UNION ALL SELECT q.e FROM {mixed_relation}) d",
            [str(2 * total), str(size - 6)],
        ),
    ):
        started = time.monotonic()
        assert db.sql(statement)["rows"] == [expected]
        assert time.monotonic() - started < 5
    for statement in (
        f"SELECT sum(d.e) FROM (SELECT q.e FROM {mixed_relation} ORDER BY 1/(q.e-q.e)) d",
        f"SELECT sum(q.e) FROM {mixed_relation} CROSS JOIN (SELECT 1/0 AS x) d",
        f"SELECT sum(d.e) FROM (SELECT CAST(q.e AS BIGINT) AS e FROM {mixed_relation} UNION ALL SELECT CAST(1/0 AS BIGINT) AS e) d",
    ):
        with pytest.raises(SQLStateError, match="22012"):
            db.sql(statement)

    split = (
        "(a IS D)-[r0 IS R]->(u0 IS D)-[r1 IS R]->(u1 IS D)-[r2 IS R]->(m IS D),"
        + "(m)-[r3 IS R]->(v0 IS D)-[r4 IS R]->(v1 IS D)-[r5 IS R]->(b IS D)"
    )
    for statement, expected in (
        (f"SELECT sum(q.score) FROM GRAPH_TABLE(g MATCH {split} COLUMNS(b.score AS score)) q", [[str(total)]]),
        (f"SELECT q.e FROM {mixed_relation} ORDER BY q.e DESC LIMIT 5 OFFSET 100", [[str(size - 1)]] * 5),
        (f"SELECT q.e FROM {mixed_relation} ORDER BY q.e LIMIT 3 OFFSET {paths - 3}", [[str(size - 1)]] * 3),
        (
            f"SELECT sum(d.e) FROM (SELECT q.e FROM {mixed_relation} ORDER BY q.e DESC LIMIT 5 OFFSET 100) d",
            [[str(5 * (size - 1))]],
        ),
        (
            f"SELECT sum(q.e*d.x),count(*) FROM {mixed_relation} CROSS JOIN (SELECT 2 AS x UNION ALL SELECT 3 AS x) d",
            [[str(5 * total), str(2 * paths)]],
        ),
        (
            f"SELECT sum(q.e),count(*) FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x) d CROSS JOIN {mixed_relation}",
            [[str(2 * total), str(2 * paths)]],
        ),
    ):
        started = time.monotonic()
        assert db.sql(statement)["rows"] == expected
        assert time.monotonic() - started < 5
    with pytest.raises(SQLStateError, match="22012"):
        db.sql(f"SELECT q.e FROM {mixed_relation} ORDER BY 1/(q.e-q.e) LIMIT 5")

    # An empty relation exhausts both negotiated passes, retaining the SQL
    # global group and its NULL sums / zero count.
    db.cypher_json("g", {"query": "MATCH ()-[r:R]->() DELETE r"})
    assert db.sql(f"SELECT sum(q.s),sum(q.e),count(*) FROM {mixed_relation}")["rows"] == [[None, None, "0"]]


def test_multi_anchor_reductions_share_work_across_bounded_batches(graph_db):
    db = graph_db
    size = 400  # Cross several scan/batch boundaries and the former work-budget cliff.
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{i + 1})" for i in range(size - 1)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for bounds in ("1..", "2..", "3..", f"1..{size}", f"2..{size}", "0.."):
        minimum = int(bounds.split("..")[0])
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (a:D)-[:R*{bounds}]->(b:D) RETURN count(*),sum(b.score),min(b.score),max(b.score)",
                "limits": {"explored_edges": size - 1, "intermediate_states": 100000, "memory_bytes": 8 << 20},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == [
            str((size - minimum) * (size - minimum + 1) // 2),
            str(sum(i * max(0, i - minimum + 1) for i in range(size))),
            str(minimum),
            str(size - 1),
        ]
        assert result["stats"]["explored_edges"] == size - 1

    for indexed in (False, True):
        if indexed:
            db.sql("CREATE INDEX docs_score ON docs(score)")
        for predicate, expected in (
            ("a.score>=0", sum(i * i for i in range(size))),
            ("b.score>=0", sum(i * i for i in range(size))),
            ("a.id IS NOT NULL", sum(i * i for i in range(size))),
            ("a.score<0", 0),
            ("a.score>=200 AND b.score<350", sum(i * (i - 200) for i in range(201, 350))),
            ("b.score<350 AND a.score>=200", sum(i * (i - 200) for i in range(201, 350))),
        ):
            filtered = db.cypher_json(
                "g",
                {
                    "query": f"MATCH (a:D)-[:R*1..]->(b:D) WHERE {predicate} RETURN sum(b.score)",
                    "limits": {"explored_edges": size - 1, "intermediate_states": 100000},
                },
            )
            assert filtered["rows"] == [[{"type": "integer", "value": str(expected)}]]
            if predicate == "a.score<0":
                assert filtered["stats"]["explored_edges"] == 0

    # The planner should make start-only consumers just as efficient as the
    # equivalent reversed pattern, without opening a less selective anchor.
    root_sum = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*1..]->(b:D) RETURN sum(a.score)",
            "limits": {"explored_edges": size - 1, "intermediate_states": 100000},
        },
    )
    assert root_sum["rows"] == [[{"type": "integer", "value": str(sum(i * (size - i - 1) for i in range(size)))}]]
    for middle in ("WITH a,b", "WITH a.id AS dead,b", "WITH a AS saved,b WITH saved AS dead,b"):
        live = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D)-[:R*1..]->(b:D) " + middle + " RETURN sum(b.score)",
                "limits": {"intermediate_states": 100000, "explored_edges": size - 1},
            },
        )
        assert live["rows"] == [[{"type": "integer", "value": str(sum(i * i for i in range(size)))}]]
    start_sum = sum(i * (size - i - 1) for i in range(size))
    end_sum = sum(i * i for i in range(size))
    for projection, expected in (
        ("sum(a.score),sum(b.score),count(*)", [start_sum, end_sum, size * (size - 1) // 2]),
        ("sum(a.score)+sum(b.score) AS total ORDER BY total", [start_sum + end_sum]),
        ("sum(a.missing),sum(b.missing),count(a.missing),count(b.missing)", [0, 0, 0, 0]),
    ):
        mixed = db.cypher_json(
            "g",
            {
                "query": "MATCH (a:D)-[:R*1..]->(b:D) RETURN " + projection,
                "limits": {"explored_edges": 2 * (size - 1), "intermediate_states": 100000},
            },
        )
        assert [cell["value"] for cell in mixed["rows"][0]] == [str(value) for value in expected]
    grouped = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*1..]->(b:D) RETURN a.score,count(*) ORDER BY a.score",
            "limits": {"explored_edges": size - 1, "intermediate_states": 100000},
        },
    )
    assert [[cell["value"] for cell in row] for row in grouped["rows"]] == [
        [str(i), str(size - i - 1)] for i in range(size - 1)
    ]

    for first in range(0, 2000, 500):
        db.sql(
            "INSERT INTO docs (_id,id,score) VALUES "
            + ",".join(f"('isolated{i}','isolated{i}',{1000 + i})" for i in range(first, first + 500))
        )
    selective = db.cypher_json(
        "g",
        {
            "query": "MATCH (a:D)-[:R*1..]->(b:D) WHERE b.score>=399 AND b.score<400 RETURN count(*),sum(b.score)",
            "limits": {"scanned_anchors": 10, "explored_edges": 399, "intermediate_states": 100000},
        },
    )
    assert [cell["value"] for cell in selective["rows"][0]] == ["399", "159201"]
    assert selective["stats"]["scanned_anchors"] == 1


def test_bounded_singleton_dag_reductions_use_proven_depth(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    size = 200
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{i + step})" for i in range(size) for step in (1, 2) if i + step < size]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for bounds in ("1..", f"1..{size}", "1..1000", f"2..{size}"):
        minimum = int(bounds.split("..")[0])
        result = db.cypher_json(
            "g",
            {
                "query": f"MATCH (a:D {{id:'n0'}})-[:R*{bounds}]->(b:D) RETURN min(b.score),max(b.score)",
                "limits": {"intermediate_states": 15000, "explored_edges": 2 * size - 3},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == [str(minimum), str(size - 1)]
    # A cap which clips legal routes must retain exact hop states.
    clipped = db.cypher_json(
        "g",
        {"query": "MATCH (a:D {id:'n0'})-[:R*1..3]->(b:D) RETURN count(*),min(b.score),max(b.score)"},
    )
    assert [cell["value"] for cell in clipped["rows"][0]] == ["14", "1", "6"]


def test_sql_weighted_reductions_batch_starting_nodes(graph_db):
    db = graph_db
    size = 400
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{i + 1})" for i in range(size - 1)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for hops in (60, 100):
        path = "(a IS D)" + "-[IS R]->(IS D)" * (hops - 1) + "-[IS R]->(b IS D)"
        # These fit the ordinary operator ceiling. Per-anchor weighted proofs
        # used to exceed it even though this chain has only size-hops routes.
        result = db.sql(
            "SELECT sum(q.score),count(*),min(q.score),max(q.score),sum(DISTINCT q.score) "
            f"FROM GRAPH_TABLE(g MATCH {path} COLUMNS(b.score AS score)) q"
        )
        total = str(sum(range(hops, size)))
        assert result["rows"] == [[total, str(size - hops), str(hops), str(size - 1), total]]
        # Reorient start-only COLUMNS too; the SQL cursor still owns one read cut.
        result = db.sql(f"SELECT sum(q.score) FROM GRAPH_TABLE(g MATCH {path} COLUMNS(a.score AS score)) q")
        assert result["rows"] == [[str(sum(range(size - hops)))]]
        # Fixed-hop frontier transfer keeps both passes below the ordinary ceiling.
        mixed_relation = (
            f"GRAPH_TABLE(g MATCH {path} COLUMNS(a.score AS s,b.score AS e,a.missing AS sm,b.missing AS em)) q"
        )
        mixed = db.sql(
            "SELECT sum(q.s),sum(q.e),count(*),avg(q.s),avg(q.e),sum(CAST(q.sm AS BIGINT)),count(q.em) FROM "
            + mixed_relation
        )
        assert mixed["rows"] == [
            [
                str(sum(range(size - hops))),
                total,
                str(size - hops),
                pytest.approx((size - hops - 1) / 2, rel=1e-14),
                pytest.approx((hops + size - 1) / 2, rel=1e-14),
                None,
                "0",
            ]
        ]
        derived = db.sql(
            f"SELECT sum(d.s),sum(d.e),count(*) FROM (SELECT q.s+1 AS s,q.e*2 AS e FROM {mixed_relation}) d"
        )
        assert derived["rows"] == [
            [str(sum(range(size - hops)) + size - hops), str(2 * sum(range(hops, size))), str(size - hops)]
        ]
        empty = db.sql(f"SELECT sum(q.s),sum(q.e),count(*) FROM {mixed_relation} WHERE false")
        assert empty["rows"] == [[None, None, "0"]]


def test_sql_weighted_seed_batches_resume_cyclic_routes(graph_db):
    db = graph_db
    size = 129  # One full seed batch plus a tail, and multiple SQL output pages.
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    for i in range(size):
        patterns += [f"(n{i})-[:R]->(n{i})", f"(n{i})-[:R]->(n{i})"]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    # Each root has two legal trails (distinct parallel self relationships).
    # Both batch and per-root acyclicity proofs decline before ordinary pulls.
    result = db.sql(
        "SELECT sum(q.score),count(*) FROM GRAPH_TABLE(g "
        "MATCH (a IS D)-[IS R]->(IS D)-[IS R]->(b IS D) COLUMNS(b.score AS score)) q"
    )
    assert result["rows"] == [[str(size * (size - 1)), str(2 * size)]]


def test_conditional_dimension_joins_preserve_graph_bags(graph_db):
    from math import comb

    db = graph_db
    size = 18
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    path = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    relation = f"GRAPH_TABLE(g MATCH {path} COLUMNS(b.score AS score)) q"
    side = "(SELECT 17 AS k UNION ALL SELECT 17 AS k UNION ALL SELECT 16 AS k UNION ALL SELECT NULL AS k) d"
    count = 2 * comb(17, 6) + comb(16, 6)
    total = 34 * comb(17, 6) + 16 * comb(16, 6)
    for source in (
        f"{relation} JOIN {side} ON q.score=d.k",
        f"{side} JOIN {relation} ON d.k=q.score",
        f"{relation} JOIN {side} ON q.score=d.k AND d.k>15",
    ):
        assert db.sql(f"SELECT count(*),sum(q.score),sum(d.k) FROM {source}")["rows"] == [
            [str(count), str(total), str(total)]
        ]
    full_count = comb(18, 7)
    full_sum = sum(i * comb(i, 6) for i in range(6, 18))
    for source in (
        f"{relation} LEFT JOIN {side} ON q.score=d.k",
        f"{side} RIGHT JOIN {relation} ON d.k=q.score",
    ):
        assert db.sql(f"SELECT count(*),sum(q.score),sum(d.k) FROM {source}")["rows"] == [
            [str(full_count + comb(17, 6)), str(full_sum + 17 * comb(17, 6)), str(total)]
        ]
    empty = "(SELECT 1 AS k WHERE false) d"
    assert db.sql(f"SELECT count(*),sum(q.score),count(d.k) FROM {relation} LEFT JOIN {empty} ON q.score=d.k")[
        "rows"
    ] == [[str(full_count), str(full_sum), "0"]]
    from antfly_embedded._sql import SQLStateError

    with pytest.raises(SQLStateError, match="22012"):
        db.sql(f"SELECT sum(q.score) FROM {relation} JOIN (SELECT 17 AS k) d ON q.score=d.k AND 1/(q.score-17)>0")


def test_sibling_branch_counts_do_not_enumerate_assignments(graph_db):
    from math import comb, perm

    db = graph_db
    patterns = [f"(n{i}:D {{score:{i}}})" for i in range(18)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(18) for j in range(i + 1, 18)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    for repeatable in (False, True):
        mode = "REPEATABLE ELEMENTS " if repeatable else ""
        for leaves in (4, 7, 16):
            query = f"MATCH {mode}(a:D)-[:R*3..3]->(m:D),(m)-[:R*3..3]->(b:D)"
            query += "".join(f",(m)-[:R]->(c{i}:D)" for i in range(leaves))
            expected = sum(
                comb(m, 3) * comb(17 - m, 3) * ((17 - m) ** leaves if repeatable else perm(16 - m, leaves))
                for m in range(3, 15)
            )
            # The 16-leaf repeatable result exceeds signed BIGINT.
            if expected > 2**63 - 1:
                continue
            result = db.cypher_json(
                "g",
                {"query": query + " RETURN count(*)", "limits": {"intermediate_states": 20000, "explored_edges": 200}},
            )
            assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]
            assert result["stats"]["intermediate_states"] < 20000
    # Overlapping, nonidentical candidate sets require subset matching rather
    # than a product of independent degrees. Two fixed leaves reserve edges.
    query = "MATCH (a:D)-[:R*3..3]->(m:D),(m)-[:R*3..3]->(b:D)"
    query += "".join(f",(m)-[:R]->(c{i}:D)" for i in range(5))
    query += ",(m)-[:R]->(x:D {score:16}),(m)-[:R]->(y:D {score:17}) RETURN count(*)"
    expected = sum(comb(m, 3) * comb(17 - m, 3) * perm(14 - m, 5) for m in range(3, 15))
    result = db.cypher_json("g", {"query": query, "limits": {"intermediate_states": 100000, "explored_edges": 200}})
    assert result["rows"] == [[{"type": "integer", "value": str(expected)}]]


def test_intermediate_windows_preserve_graph_bags(graph_db):
    import time

    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    layers = 64
    patterns = ["(s:D {id:'start',score:0})"] + [
        f"(v{i}_{j}:D {{score:{i + 1}}})" for i in range(layers) for j in range(2)
    ]
    patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
    patterns += [f"(v{i}_{j})-[:R]->(v{i + 1}_{k})" for i in range(layers - 1) for j in range(2) for k in range(2)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    path = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(63)) + "-[r63 IS R]->(b IS D)"
    relation = f"GRAPH_TABLE(g MATCH {path} WHERE a.id='start' COLUMNS(b.score AS score)) q"
    for order in ("", "ORDER BY q.score DESC"):
        started = time.monotonic()
        window = f"SELECT q.score FROM {relation} {order} LIMIT 5000000 OFFSET 1000"
        assert db.sql(f"SELECT count(*) FROM ({window}) d")["rows"] == [["5000000"]]
        assert db.sql(f"SELECT count(*),sum(d.score),min(d.score) FROM ({window}) d")["rows"] == [
            ["5000000", "320000000", "64"]
        ]
        assert db.sql(f"SELECT count(*),sum(e.score) FROM (SELECT d.score FROM ({window}) d LIMIT 123 OFFSET 17) e")[
            "rows"
        ] == [["123", "7872"]]
        assert time.monotonic() - started < 5
    # Overflow sentinels on both inputs must survive the private build/spill
    # weight codec and multiplication before the finite consumer clips them.
    other = relation.replace("b.score AS score)) q", "b.score AS k)) d")
    joined = f"SELECT q.score FROM {relation} JOIN {other} ON q.score=d.k ORDER BY q.score LIMIT 3 OFFSET 5"
    assert db.sql(f"SELECT count(*),sum(score) FROM ({joined}) t")["rows"] == [["3", "192"]]
    for order in ("", "ORDER BY b.score DESC"):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) "
                f"WITH b {order} SKIP 1000 LIMIT 5000000 "
                "WITH b SKIP 17 LIMIT 123 UNWIND [1,2] AS k RETURN count(*),sum(b.score),sum(k)",
                "limits": {"intermediate_states": 10000, "explored_edges": 300},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == ["246", "15744", "369"]
    for tail, expected in (
        ("RETURN DISTINCT b.score", ["64"]),
        ("MATCH (b)-[:R]->(c:D) RETURN count(*)", ["0"]),
        ("OPTIONAL MATCH (b)-[:R]->(c:D) RETURN count(*),count(c)", ["5000000", "0"]),
        ("WITH DISTINCT * RETURN sum(b.score)", ["64"]),
        ("MATCH (b)-[:R]->(c:D) WITH DISTINCT * RETURN count(*)", ["0"]),
        ("WITH DISTINCT b.score AS score RETURN count(*),sum(score)", ["1", "64"]),
        ("WITH b.score AS score WHERE score=64 RETURN count(*),sum(score)", ["5000000", "320000000"]),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) WITH b ORDER BY b.score LIMIT 5000000 " + tail,
                "limits": {"intermediate_states": 10000, "explored_edges": 300},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == expected
    window = "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) WITH b ORDER BY b.score LIMIT 5000000 "
    for tail, expected in (
        ("WITH b WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*)", ["0"]),
        ("WITH b WHERE NOT EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*),sum(b.score)", ["5000000", "320000000"]),
        ("RETURN count(EXISTS { MATCH (b)-[:R]->(c:D) RETURN count(*) })", ["5000000"]),
        ("WITH b,[(b)-[:R]->(c:D) | c.score] AS xs UNWIND xs AS x RETURN count(*)", ["0"]),
        ("MATCH (b)<-[:R*24..24]-(c:D) RETURN count(*)", ["83886080000000"]),
        ("MATCH p=(b)<-[:R*24..24]-(c:D) WITH c WITH * RETURN count(*)", ["83886080000000"]),
        ("MATCH (b)<-[:R*24..24]-(c:D) RETURN sum(c.score)", ["3355443200000000"]),
        ("OPTIONAL MATCH (b)<-[:R*24..24]-(c:D) RETURN count(*),count(c)", ["83886080000000", "83886080000000"]),
        ("MATCH (b)<-[:R*64..64]-(c:D) WITH DISTINCT c RETURN count(*)", ["1"]),
        ("MATCH (b)<-[:R*64..64]-(c:D) RETURN min(c.score),max(c.score)", ["0", "0"]),
        ("MATCH (b)<-[:R*64..64]-(c:D) RETURN sum(DISTINCT c.score),avg(DISTINCT c.score)", ["0", 0.0]),
        ("MATCH (b)<-[:R*64..64]-(c:D) RETURN min(c.score)+max(c.score)", ["0"]),
        ("MATCH (b)<-[:R*24..24]-(c:D) RETURN count(*)+1", ["83886080000001"]),
        ("CALL { WITH b MATCH (b)-[:R]->(c:D) RETURN c } RETURN count(*)", ["0"]),
        ("CALL { WITH b OPTIONAL MATCH (b)-[:R]->(c:D) RETURN c } RETURN count(*),count(c)", ["5000000", "0"]),
        ("CALL { WITH b MATCH (b)-[:R]->(c:D) } RETURN count(*)", ["5000000"]),
        ("CALL { WITH b MATCH (b)<-[:R*64..64]-(c:D) } RETURN count(*)", ["5000000"]),
        ("CALL { WITH b MATCH (b)<-[:R*64..64]-(c:D) RETURN c LIMIT 3 } RETURN count(*)", ["15000000"]),
        ("CALL { WITH b MATCH (b)-[:R]->(c:D) RETURN count(*) AS n } RETURN count(*),sum(n)", ["5000000", "0"]),
        ("CALL { WITH b MATCH (b)<-[:R*24..24]-(c:D) RETURN c } RETURN count(*)", ["83886080000000"]),
        ("CALL { WITH b MATCH (b)<-[:R*64..64]-(c:D) RETURN c } RETURN min(c.score)", ["0"]),
        ("CALL { WITH b CALL { WITH b RETURN b.score AS n } RETURN n } RETURN sum(n)", ["320000000"]),
        ("MATCH (b)<-[:R*0..64]-(c:D) RETURN count(DISTINCT CASE WHEN c.score=0 THEN 1 END)", ["1"]),
    ):
        result = db.cypher_json(
            "g", {"query": window + tail, "limits": {"intermediate_states": 15000, "explored_edges": 1000}}
        )
        assert [cell["value"] for cell in result["rows"][0]] == expected

    for hops in (48, 64):
        for goal, parameters in (("3", {}), ("$take", {"take": 3}), ("$take+1", {"take": 2})):
            result = db.cypher_json(
                "g",
                {
                    "query": window
                    + f"MATCH (b)<-[:R*{hops}..{hops}]-(c:D) WITH c SKIP $skip LIMIT {goal} RETURN count(*)",
                    "parameters": {**parameters, "skip": 2},
                    "limits": {"intermediate_states": 10000, "explored_edges": 1000},
                },
            )
            assert result["rows"] == [[{"type": "integer", "value": "3"}]]
    result = db.cypher_json(
        "g",
        {
            "query": window + "MATCH (b)<-[:R*48..48]-(c:D) WITH c LIMIT $take RETURN count(*)",
            "parameters": {"take": 0},
            "limits": {"intermediate_states": 10000, "explored_edges": 1000},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "0"}]]
    for take in (-1, None, "3"):
        with pytest.raises(af.GraphError, match="InvalidCypherLimit"):
            db.cypher_json(
                "g",
                {
                    "query": window + "MATCH (b)<-[:R*8..8]-(c:D) WITH c LIMIT $take RETURN count(*)",
                    "parameters": {"take": take},
                },
            )
    for body in ("RETURN 1 AS b", "WITH b RETURN b"):
        with pytest.raises(af.GraphError, match="CypherVariableConflict"):
            db.cypher_json("g", {"query": window + f"CALL {{ {body} }} RETURN count(*)"})
    result = db.cypher_json(
        "g",
        {
            "query": window + "MATCH (b)<-[:R*64..64]-(c:D) RETURN collect(DISTINCT c.score)",
            "limits": {"intermediate_states": 10000, "explored_edges": 1000},
        },
    )
    assert result["rows"] == [[{"type": "list", "value": [{"type": "integer", "value": "0"}]}]]
    base = "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) "
    for tail in (
        "WITH b WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*)",
        "WITH b,[(b)-[:R]->(c:D) | c.score] AS xs UNWIND xs AS x RETURN count(*)",
        "UNWIND [(b)-[:R]->(c:D) | c.score] AS x RETURN count(*)",
        "CALL { WITH b MATCH (b)-[:R]->(c:D) RETURN c } RETURN count(*)",
    ):
        result = db.cypher_json(
            "g", {"query": base + tail, "limits": {"intermediate_states": 10000, "explored_edges": 1000}}
        )
        assert result["rows"] == [[{"type": "integer", "value": "0"}]]
    result = db.cypher_json(
        "g",
        {
            "query": base + "RETURN size(collect(DISTINCT b.score))",
            "limits": {"intermediate_states": 10000, "explored_edges": 1000},
        },
    )
    assert result["rows"] == [[{"type": "integer", "value": "1"}]]
    for verb, tail, expected in (
        ("MATCH", "WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*)", ["0"]),
        ("MATCH", "WHERE NOT EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(DISTINCT b)", ["2"]),
        ("OPTIONAL MATCH", "WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*),count(b)", ["1", "0"]),
    ):
        result = db.cypher_json(
            "g",
            {
                "query": verb + base.removeprefix("MATCH") + tail,
                "limits": {"intermediate_states": 10000, "explored_edges": 1000},
            },
        )
        assert [cell["value"] for cell in result["rows"][0]] == expected
    result = db.cypher_json(
        "g",
        {
            "query": window
            + "OPTIONAL MATCH (b)<-[:R*64..64]-(c:D) WHERE EXISTS { MATCH (c)-[:R]->(d:D) WHERE false } RETURN count(*),count(c)",
            "limits": {"intermediate_states": 10000, "explored_edges": 1000},
        },
    )
    assert [cell["value"] for cell in result["rows"][0]] == ["5000000", "0"]
    for tail in (
        "CALL { WITH b RETURN 1/0 AS n } RETURN count(*)",
        "WITH b WHERE EXISTS { MATCH (b)-[:R]->(c:D) WHERE false } AND 1/0=1 RETURN count(*)",
    ):
        with pytest.raises(af.AntflyError, match="CypherDivisionByZero"):
            db.cypher_json("g", {"query": window + tail})

    for query, expected in (
        (window + "MATCH (b)<-[:R*48..48]-(c:D) RETURN avg(c.score),sum(0),count(c.missing)", [16.0, "0", "0"]),
        (window + "MATCH (b)<-[:R*48..48]-(c:D) WITH c RETURN avg(c.score)", [16.0]),
        (window + "CALL { WITH b MATCH (b)<-[:R*48..48]-(c:D) RETURN c } RETURN avg(c.score)", [16.0]),
        (
            window + "MATCH (b)<-[:R*48..48]-(c:D) RETURN sum(1e-300)",
            [pytest.approx(5000000 * 2**48 * 1e-300, rel=1e-12, abs=0)],
        ),
        (
            base
            + "WITH b LIMIT 2 MATCH (b)<-[:R*64..64]-(c:D) WITH c SKIP 9223372036854775807 WITH c LIMIT 3 RETURN count(*)",
            ["3"],
        ),
        ("CALL { " + base + "RETURN b } RETURN count(DISTINCT b)", ["2"]),
        ("WITH 1 AS k CALL { " + base + "RETURN b ORDER BY b.score LIMIT 5000000 } RETURN count(*)", ["5000000"]),
        ("CALL { " + base + "RETURN b ORDER BY b.score LIMIT 5000000 } RETURN count(*)", ["5000000"]),
        ("CALL { " + base + " } RETURN count(*)", ["1"]),
        (base.replace("MATCH ", "MATCH p=", 1) + "WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*)", ["0"]),
        (
            base.replace("MATCH ", "MATCH p=", 1) + "WITH b WHERE EXISTS { MATCH (b)-[:R]->(c:D) } RETURN count(*)",
            ["0"],
        ),
        (base.replace("MATCH ", "MATCH p=", 1) + "RETURN min(size([p=(b)-[:R]->(c:D) | length(p)]))", ["0"]),
        (
            base.replace("MATCH ", "MATCH p=", 1)
            + "RETURN min(CASE WHEN EXISTS { WITH 1 AS p RETURN p } THEN 1 ELSE 0 END)",
            ["1"],
        ),
        ("MATCH (s:D {id:'start'}) RETURN size([(s)-[:R*24..24]->(c:D) | c.score])", ["16777216"]),
        ("MATCH (s:D {id:'start'}) RETURN size([(s)-[:R*24..24]->(c:D) | NULL])", ["16777216"]),
        ("MATCH (s:D {id:'start'}) RETURN size([(s)<-[:R*24..24]-(c:D) | 1/0])", ["0"]),
        (base + "WITH b ORDER BY b.score RETURN min(b.score)+max(b.score)", ["128"]),
        (base + "WITH b ORDER BY b.score RETURN count(DISTINCT b)+1", ["3"]),
    ):
        result = db.cypher_json("g", {"query": query, "limits": {"intermediate_states": 10000, "explored_edges": 1000}})
        assert [cell["value"] for cell in result["rows"][0]] == expected, query
    enormous = (
        window
        + "MATCH (b)<-[:R*48..48]-(c:D) MATCH (c)-[:R*48..48]->(d:D) MATCH (d)<-[:R*48..48]-(e:D) RETURN avg(e.score),sum(0)"
    )
    result = db.cypher_json("g", {"query": enormous, "limits": {"intermediate_states": 50000, "explored_edges": 4000}})
    assert [cell["value"] for cell in result["rows"][0]] == [16.0, "0"]
    small_goal = db.cypher_json(
        "g",
        {
            "query": base + "RETURN b.score SKIP 1 LIMIT 1",
            "limits": {"intermediate_states": 2000, "explored_edges": 150},
        },
    )
    assert small_goal["rows"] == [[{"type": "integer", "value": "64"}]]
    for order in ("", "ORDER BY b.score"):
        result = db.cypher_json(
            "g",
            {
                "query": base + f"RETURN b.score {order} SKIP 9223372036854775807 LIMIT 3",
                "limits": {"intermediate_states": 10000, "explored_edges": 1000},
            },
        )
        assert result["rows"] == [[{"type": "integer", "value": "64"}]] * 3
    for query, code in (
        (window + "MATCH (b)<-[:R*48..48]-(c:D) RETURN sum(1)", "CypherNumericOverflow"),
        (window + "MATCH (b)<-[:R*48..48]-(c:D) RETURN collect(0)", "CypherStateBudgetExceeded"),
        ("MATCH (s:D {id:'start'}) RETURN size([(s)-[:R*24..24]->(c:D) | 1/0])", "CypherDivisionByZero"),
        (
            base.replace("*64..64", "*4..4") + "WITH b ORDER BY 1/0 RETURN min(b.score)+max(b.score)",
            "CypherDivisionByZero",
        ),
    ):
        with pytest.raises(af.GraphError) as failed:
            db.cypher_json("g", {"query": query, "limits": {"intermediate_states": 10000, "explored_edges": 1000}})

        assert code in str(failed.value), query

    empty_binding = db.cypher_json(
        "g",
        {"query": "MATCH (:D {id:'start'})-[:R*64..64]->(:D) WITH * LIMIT 5000000 WITH DISTINCT * RETURN count(*)"},
    )
    assert empty_binding["rows"] == [[{"type": "integer", "value": "1"}]]
    collected = db.cypher_json(
        "g",
        {
            "query": "MATCH (s:D {id:'start'})-[:R*64..64]->(b:D) WITH b ORDER BY b.score LIMIT 7 RETURN collect(b.score)"
        },
    )
    assert collected["rows"] == [[{"type": "list", "value": [{"type": "integer", "value": "64"}] * 7}]]


def test_ordered_graph_windows_cover_the_entire_signed_offset_range(graph_db):
    db = graph_db
    db.sql("CREATE INDEX docs_id ON docs(id)")
    patterns = ["(s:D {id:'start',score:0})"]
    patterns += [f"(v{i}_{j}:D {{score:{i + 1}}})" for i in range(64) for j in range(2)]
    patterns += ["(a:D {id:'a',score:65})", "(z:D {id:'z',score:65})"]
    patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
    patterns += [f"(v{i}_{j})-[:R]->(v{i + 1}_{k})" for i in range(63) for j in range(2) for k in range(2)]
    patterns += [f"(v63_{j})-[:R]->({end})" for j in range(2) for end in ("a", "z")]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    base = "MATCH (s:D {id:'start'})-[:R*65..65]->(b:D) "
    for projection, tail in (("RETURN b.id", ""), ("WITH b", " RETURN b.id")):
        for goal in ("SKIP 9223372036854775807 LIMIT 3", "SKIP $skip LIMIT $take"):
            result = db.cypher_json(
                "g",
                {
                    "query": base + projection + " ORDER BY b.id " + goal + tail,
                    "parameters": {"skip": 2**63 - 1, "take": 3},
                    "limits": {"intermediate_states": 10000, "explored_edges": 1000},
                },
            )
            assert result["rows"] == [[{"type": "string", "value": "a"}]] * 3


def test_persisted_dimensions_membership_and_outer_joins_keep_weights(graph_db):
    import time
    from math import comb

    db = graph_db
    size = 18
    patterns = [f"(n{i}:D {{id:'n{i}',score:{i}}})" for i in range(size)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(size) for j in range(i + 1, size)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    db.sql("CREATE TABLE dim(k BIGINT)")
    db.sql("INSERT INTO dim(k) VALUES(16),(17),(17),(NULL),(99)")
    path = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    relation = f"GRAPH_TABLE(g MATCH {path} COLUMNS(b.score AS score)) q"
    matched_count = comb(16, 6) + 2 * comb(17, 6)
    matched_sum = 16 * comb(16, 6) + 34 * comb(17, 6)
    full_count = comb(18, 7)
    full_sum = sum(i * comb(i, 6) for i in range(6, 18))
    cases = (
        (f"{relation} JOIN dim d ON q.score=d.k", matched_count, matched_sum, matched_sum),
        (f"dim d JOIN {relation} ON q.score=d.k", matched_count, matched_sum, matched_sum),
        (
            f"{relation} FULL JOIN dim d ON q.score=d.k",
            full_count + comb(17, 6) + 2,
            full_sum + 17 * comb(17, 6),
            matched_sum + 99,
        ),
        (f"dim d LEFT JOIN {relation} ON q.score=d.k", matched_count + 2, matched_sum, matched_sum + 99),
        (
            f"{relation} RIGHT JOIN dim d ON q.score=d.k AND q.score<17",
            comb(16, 6) + 4,
            16 * comb(16, 6),
            16 * comb(16, 6) + 133,
        ),
    )
    for source, count, total, dimension_total in cases:
        started = time.monotonic()
        assert db.sql(f"SELECT count(*),sum(q.score),sum(d.k) FROM {source}")["rows"] == [
            [str(count), str(total), str(dimension_total)]
        ]
        assert time.monotonic() - started < 5
    for predicate, count in (
        ("IN (SELECT k FROM dim)", comb(16, 6) + comb(17, 6)),
        ("NOT IN (SELECT k FROM dim)", 0),
        ("NOT IN (SELECT k FROM dim WHERE k IS NOT NULL)", full_count - comb(16, 6) - comb(17, 6)),
    ):
        started = time.monotonic()
        assert db.sql(f"SELECT count(*) FROM {relation} WHERE q.score {predicate}")["rows"] == [[str(count)]]
        assert time.monotonic() - started < 5
    # Both inputs are graph producers. The build must retain its own bag
    # weights, including duplicate endpoints, rather than decode every route.
    vertices = "GRAPH_TABLE(g MATCH(d IS D) COLUMNS(d.score AS k)) d"
    started = time.monotonic()
    assert db.sql(f"SELECT count(*),sum(q.score) FROM {relation} JOIN {vertices} ON q.score=d.k")["rows"] == [
        [str(full_count), str(full_sum)]
    ]
    weighted = f"GRAPH_TABLE(g MATCH {path} COLUMNS(b.score AS k)) d"
    expected_count = sum(comb(i, 6) ** 2 for i in range(6, 18))
    expected_sum = sum(i * comb(i, 6) ** 2 for i in range(6, 18))
    assert db.sql(f"SELECT count(*),sum(q.score) FROM {relation} JOIN {weighted} ON q.score=d.k")["rows"] == [
        [str(expected_count), str(expected_sum)]
    ]
    assert time.monotonic() - started < 5
    empty_graph = f"(SELECT q.score FROM {relation} WHERE q.score<0) q"
    assert db.sql(f"SELECT count(*),count(q.score),sum(d.k) FROM {empty_graph} FULL JOIN dim d ON q.score=d.k")[
        "rows"
    ] == [["5", "0", "149"]]
    db.sql("DELETE FROM dim")
    assert db.sql(f"SELECT count(*) FROM {relation} FULL JOIN dim d ON q.score=d.k")["rows"] == [[str(full_count)]]
    for operator, expected in (("IN", 0), ("NOT IN", full_count)):
        assert db.sql(f"SELECT count(*) FROM {relation} WHERE q.score {operator}(SELECT k FROM dim)")["rows"] == [
            [str(expected)]
        ]


def test_materialized_graph_ctes_share_exact_weighted_replay(graph_db):
    import time
    from math import comb

    db = graph_db
    patterns = [f"(n{i}:D {{score:{i}}})" for i in range(18)]
    patterns += [f"(n{i})-[:R]->(n{j})" for i in range(18) for j in range(i + 1, 18)]
    db.cypher_json("g", {"query": "CREATE " + ",".join(patterns)})
    path = "(a IS D)" + "".join(f"-[r{i} IS R]->(m{i} IS D)" for i in range(5)) + "-[r5 IS R]->(b IS D)"
    source = f"SELECT q.s,q.e FROM GRAPH_TABLE(g MATCH {path} COLUMNS(a.score AS s,b.score AS e)) q"
    count = comb(18, 7)
    total = sum(i * comb(i, 6) for i in range(6, 18))
    start_total = sum(i * comb(17 - i, 6) for i in range(12))
    for directive in ("MATERIALIZED", "NOT MATERIALIZED"):
        started = time.monotonic()
        prefix = f"WITH paths AS {directive} ({source}) "
        assert db.sql(prefix + "SELECT count(*),sum(s),sum(e) FROM paths")["rows"] == [
            [str(count), str(start_total), str(total)]
        ]
        assert time.monotonic() - started < 5
    # Automatic multi-reference capture and explicit capture both preserve
    # independent reader state and the union of consumers' dependencies.
    for directive in ("", "MATERIALIZED"):
        started = time.monotonic()
        prefix = f"WITH paths AS {directive} ({source}) "
        result = db.sql(prefix + "SELECT sum(s) AS n FROM paths UNION ALL SELECT sum(e) AS n FROM paths")
        assert result["rows"] == [[str(start_total)], [str(total)]]
        assert time.monotonic() - started < 5
    # A row consumer expands only what its own LIMIT demands.
    assert len(db.sql(f"WITH paths AS MATERIALIZED ({source}) SELECT * FROM paths LIMIT 7")["rows"]) == 7

    # A shared replay also serves ordinary consumers. Preserve ordering in
    # captures whose aggregate-only proof would otherwise omit the sort.
    ordered = (
        "SELECT q.e FROM GRAPH_TABLE(g MATCH (a IS D)-[r IS R]->(b IS D) COLUMNS(b.score AS e)) q ORDER BY q.e DESC"
    )
    assert db.sql(f"WITH paths AS MATERIALIZED({ordered}) SELECT e FROM paths LIMIT 7")["rows"] == [["17"]] * 7
