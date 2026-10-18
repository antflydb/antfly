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

"""Native graph access-path and staged SQL regression campaigns."""

import json
import os
import subprocess
import sys
import time
import uuid
from math import comb, perm
from pathlib import Path

import conftest
import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    find_free_port,
)


def test_authenticated_cypher_property_plans_and_sql_overlays(monkeypatch):
    monkeypatch.setenv(
        "ANTFLY_TRUSTED_PRINCIPAL_SECRET", "cypher-performance-principal-secret-v1"
    )
    monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_ISSUER", "cypher-performance")
    write_config = conftest._write_remote_content_e2e_config

    def with_graph_work_budget(*args, **kwargs):
        path = write_config(*args, **kwargs)
        config = json.loads(path.read_text())
        config["graph_execution"] = {"max_intermediate_states": 1000000}
        path.write_text(json.dumps(config))
        return path

    monkeypatch.setattr(
        conftest, "_write_remote_content_e2e_config", with_graph_work_budget
    )
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    session = requests.Session()
    session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    table = "cypher_access_" + uuid.uuid4().hex
    graph = table + "_g"
    try:

        def call(path, body, *, wait_for_index=False):
            deadline = time.monotonic() + 60
            while True:
                response = session.post(
                    server.api_url.rstrip("/") + path, json=body, timeout=45
                )
                if not (
                    wait_for_index
                    and response.status_code == 429
                    and response.json().get("code") == "CypherAnchorBudgetExceeded"
                    and time.monotonic() < deadline
                ):
                    break
                # SQL index publication precedes asynchronous owner backfill.
                # Retry only this read until its strict budget proves READY use.
                time.sleep(0.1)
            assert response.status_code in (200, 201, 202), (
                response.status_code,
                response.text,
                server.log_path.read_text()[-6000:],
            )
            return response.json()

        call("/sql", {"statement": f"CREATE TABLE {table} (id TEXT, score BIGINT)"})
        call("/sql", {"statement": f"CREATE INDEX {table}_id ON {table}(id)"})
        call("/sql", {"statement": f"CREATE INDEX {table}_score ON {table}(score)"})
        call(
            f"/tables/{table}/batch",
            {
                "inserts": {f"n{i}": {"id": f"n{i}", "score": i} for i in range(2000)},
                "sync_level": "full_index",
            },
        )
        call(
            f"/graphs/{graph}",
            {"labels": [{"name": "D", "table": table}], "relationships": []},
        )
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D {id:'n1999'}) RETURN n.score",
                "limits": {"scanned_anchors": 4},
            },
            wait_for_index=True,
        )
        assert result["rows"] == [[{"type": "integer", "value": "1999"}]], result
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) WITH n.score AS s RETURN s%2 AS bucket,count(*) AS total ORDER BY bucket",
                "limits": {"memory_bytes": 8 << 20},
            },
        )
        assert [[cell["value"] for cell in row] for row in result["rows"]] == [
            ["0", "1000"],
            ["1", "1000"],
        ], result
        first_memory = result["stats"]["memory_bytes"]
        distinct = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) RETURN DISTINCT n.score%2 AS bucket LIMIT 1",
                "limits": {"scanned_anchors": 1, "memory_bytes": 2 << 20},
            },
        )
        assert len(distinct["rows"]) == 1, distinct
        assert distinct["stats"]["scanned_anchors"] == 1, distinct
        joined = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (a:D),(b:D) WHERE a.score<150 AND a.score=b.score RETURN count(*)",
                "limits": {"scanned_anchors": 400, "memory_bytes": 8 << 20},
            },
            wait_for_index=True,
        )
        assert joined["rows"] == [[{"type": "integer", "value": "150"}]], joined
        assert joined["stats"]["scanned_anchors"] <= 400, joined
        joined_unwind = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (a:D),(b:D) WHERE a.score<150 AND a.score=b.score UNWIND [1] AS x RETURN count(*)",
                "limits": {"scanned_anchors": 400, "memory_bytes": 8 << 20},
            },
            wait_for_index=True,
        )
        assert joined_unwind["rows"] == [[{"type": "integer", "value": "150"}]], (
            joined_unwind
        )
        assert joined_unwind["stats"]["scanned_anchors"] <= 400, joined_unwind
        for match in (
            "MATCH (a:D) WHERE a.score<150 MATCH (b:D) WHERE a.score=b.score",
            "OPTIONAL MATCH (a:D),(b:D) WHERE a.score<150 AND a.score=b.score",
        ):
            bounded_join = call(
                f"/graphs/{graph}/query",
                {
                    "query": match + " RETURN count(b)",
                    "limits": {"scanned_anchors": 400, "memory_bytes": 8 << 20},
                },
                wait_for_index=True,
            )
            assert bounded_join["rows"] == [[{"type": "integer", "value": "150"}]], (
                bounded_join
            )
        for predicate, anchors, values in (
            ("n.score>=1999", 1, ["1999"]),
            ("n.score=-(-1999)", 1, ["1999"]),
            ("n.score=2001-2", 1, ["1999"]),
            ("n.score IN [1999,1,1999,null]", 2, ["1", "1999"]),
        ):
            indexed = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"MATCH (n:D) WHERE {predicate} RETURN n.score ORDER BY n.score",
                    "limits": {"scanned_anchors": anchors},
                },
                wait_for_index=True,
            )
            assert [row[0]["value"] for row in indexed["rows"]] == values, indexed
            assert indexed["stats"]["scanned_anchors"] == anchors, indexed

        call(
            f"/tables/{table}/batch",
            {
                "inserts": {
                    f"n{i}": {"id": f"n{i}", "score": i} for i in range(2000, 10000)
                },
                "sync_level": "full_index",
            },
        )
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) WITH n.score AS s RETURN s%2 AS bucket,count(*) AS total ORDER BY bucket",
                "limits": {"memory_bytes": 8 << 20},
            },
        )
        assert [[cell["value"] for cell in row] for row in result["rows"]] == [
            ["0", "5000"],
            ["1", "5000"],
        ], result
        assert result["stats"]["memory_bytes"] <= first_memory + (1 << 20), result
        distinct_aggregate = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) RETURN count(DISTINCT n.score%2),sum(DISTINCT n.score%2)",
                "limits": {"memory_bytes": 8 << 20},
            },
        )
        assert [cell["value"] for cell in distinct_aggregate["rows"][0]] == [
            "2",
            "1",
        ], distinct_aggregate
        collected = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) WITH DISTINCT n.score%2 AS bucket RETURN collect(bucket)",
                "limits": {"memory_bytes": 8 << 20},
            },
        )
        assert {value["value"] for value in collected["rows"][0][0]["value"]} == {
            "0",
            "1",
        }, collected
        # Column requirements must survive the coordinator/owner RPC boundary.
        wide = table + "_wide"
        call("/sql", {"statement": f"CREATE TABLE {wide} (score BIGINT,payload TEXT)"})
        call(
            f"/tables/{wide}/batch",
            {
                "inserts": {
                    f"n{i}": {"score": i, "payload": "x" * 8192} for i in range(400)
                },
                "sync_level": "full_index",
            },
        )
        call(
            f"/graphs/{wide}",
            {"labels": [{"name": "W", "table": wide}], "relationships": []},
        )
        # Retained owner/RPC cuts have a larger fixed cost than embedded
        # reads; hydrating the unused payload still exceeds this ceiling.
        for forwarding in ("", "WITH n AS m WITH m AS n "):
            pruned = call(
                f"/graphs/{wide}/query",
                {
                    "query": f"MATCH (n:W) {forwarding}RETURN count(*),sum(n.score)",
                    "limits": {"memory_bytes": 8 << 20},
                },
            )
            assert [cell["value"] for cell in pruned["rows"][0]] == ["400", "79800"], (
                pruned
            )
        complete = call(
            f"/graphs/{wide}/query",
            {"query": "MATCH (n:W) WHERE antfly.documentKey(n)='n0' RETURN n.payload"},
        )
        assert complete["rows"] == [[{"type": "string", "value": "x" * 8192}]], complete

        relation = f"GRAPH_TABLE({wide} MATCH (n IS W) COLUMNS(n.score AS score,n.payload AS unused)) g"
        for statement, expected in (
            (f"SELECT g.score FROM {relation} WHERE g.score=399", [["399"]]),
            (f"SELECT count(*) FROM {relation}", [["400"]]),
            (f"SELECT g.unused FROM {relation} WHERE g.score=399", [["x" * 8192]]),
            (f"SELECT g.score FROM {relation} LIMIT 1", [["0"]]),
            (
                (
                    f"SELECT a.score,b.score FROM {relation.replace(') g', ') a')} "
                    f"CROSS JOIN GRAPH_TABLE({graph} MATCH (n IS D) COLUMNS(n.score AS score)) b "
                    "WHERE a.score=399 AND b.score=1999"
                ),
                [["399", "1999"]],
            ),
        ):
            sql = call("/sql", {"statement": statement})
            assert sql["rows"] == expected, sql

        for source, predicate in (
            (relation, "g.score IN (398,399,399,NULL)"),
            (relation, "g.score=398 OR g.score=399"),
            (f"(SELECT g.score,g.unused FROM {relation}) g", "g.score IN (398,399)"),
        ):
            filtered = call(
                "/sql",
                {
                    "statement": f"SELECT g.score,g.unused FROM {source} WHERE {predicate} ORDER BY g.score"
                },
            )
            assert filtered["rows"] == [["398", "x" * 8192], ["399", "x" * 8192]], (
                filtered
            )

        hops = table + "_hops"
        call("/sql", {"statement": f"CREATE TABLE {hops} (id TEXT,score BIGINT)"})
        call(
            f"/tables/{hops}/indexes/links",
            {"type": "graph", "edge_types": [{"name": "link"}]},
        )
        call(
            f"/graphs/{hops}",
            {
                "labels": [{"name": "H", "table": hops}],
                "relationships": [
                    {
                        "name": "R",
                        "index": "links",
                        "source_label": "H",
                        "target_label": "H",
                        "stored_type": "link",
                    }
                ],
            },
        )
        patterns = [f"(n{i}:H {{id:'n{i}',score:{i}}})" for i in range(18)]
        patterns += [
            f"(n{i})-[:R]->(n{j})" for i in range(18) for j in range(i + 1, 18)
        ]
        call(f"/graphs/{hops}/query", {"query": "CREATE " + ",".join(patterns)})
        path = (
            "(a IS H)"
            + "".join(f"-[r{i} IS R]->(m{i} IS H)" for i in range(5))
            + "-[r5 IS R]->(b IS H)"
        )
        pulled = call(
            "/sql",
            {
                "statement": f"SELECT q.score FROM GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q LIMIT 1"
            },
        )
        assert len(pulled["rows"]) == 1, pulled
        call("/sql", {"statement": f"CREATE INDEX {hops}_score ON {hops}(score)"})
        # Publication precedes asynchronous backfill. Prove READY use before
        # timing the selective interior plan, as for the earlier property seeks.
        ready = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (n:H) WHERE n.score=9 RETURN n.score",
                "limits": {"scanned_anchors": 1},
            },
            wait_for_index=True,
        )
        assert ready["rows"] == [[{"type": "integer", "value": "9"}]], ready
        interior = call(
            "/sql",
            {
                "statement": f"SELECT q.score FROM GRAPH_TABLE({hops} MATCH {path} WHERE m2.score=9 COLUMNS(b.score AS score)) q LIMIT 1"
            },
        )
        assert len(interior["rows"]) == 1, interior
        counted = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) RETURN count(*)",
                "limits": {"explored_edges": 1000, "intermediate_states": 20000},
            },
        )
        assert counted["rows"] == [[{"type": "integer", "value": "31824"}]], counted
        for tail in (
            "RETURN count(b)",
            "WHERE b.score>=0 RETURN count(*)",
            "RETURN b.score,count(*) ORDER BY b.score",
        ):
            endpoint_counted = call(
                f"/graphs/{hops}/query",
                {
                    "query": "MATCH (a:H)-[:R*6..6]->(b:H) " + tail,
                    "limits": {"explored_edges": 1000, "intermediate_states": 30000},
                },
            )
            if tail.startswith("RETURN b.score"):
                assert (
                    sum(int(row[1]["value"]) for row in endpoint_counted["rows"])
                    == 31824
                )
            else:
                assert endpoint_counted["rows"] == [
                    [{"type": "integer", "value": "31824"}]
                ]
        statistics = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) RETURN count(*),sum(b.score),min(b.score),max(b.score),count(DISTINCT b)",
                "limits": {"explored_edges": 1000, "intermediate_states": 30000},
            },
        )
        assert statistics["rows"][0][0] == {"type": "integer", "value": "31824"}, (
            statistics
        )
        assert statistics["rows"][0][-1] == {"type": "integer", "value": "12"}, (
            statistics
        )
        grouped_alias = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) RETURN b.score AS endpoint,count(*) AS total ORDER BY endpoint",
                "limits": {"explored_edges": 1000, "intermediate_states": 30000},
            },
        )
        assert sum(int(row[1]["value"]) for row in grouped_alias["rows"]) == 31824
        start = time.monotonic()
        counted_sql = call(
            "/sql",
            {
                "statement": f"SELECT count(*) FROM GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q"
            },
        )
        assert counted_sql["rows"] == [["31824"]], counted_sql
        assert time.monotonic() - start < 10
        start = time.monotonic()
        weighted_sql = call(
            "/sql",
            {
                "statement": f"SELECT sum(q.score),min(q.score),max(q.score),sum(DISTINCT q.score) FROM GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q"
            },
        )
        assert weighted_sql["rows"] == [["497250", "6", "17", "138"]], weighted_sql
        assert time.monotonic() - start < 10
        # Exercise the weighted operator tree through both authenticated SQL
        # and pgwire: projections, an unobserved sort, singleton joins and UNION ALL.
        leaf = (
            f"SELECT q.s+1 AS s,q.e*2 AS e FROM GRAPH_TABLE({hops} MATCH {path} "
            "COLUMNS(a.score AS s,b.score AS e)) q CROSS JOIN (SELECT 1 AS k) constant"
        )
        mixed_statement = (
            "SELECT sum(d.s),sum(d.e),count(*),avg(d.s),avg(d.e) FROM "
            f"(SELECT s,e FROM ({leaf} ORDER BY q.e) ordered "
            f"UNION ALL SELECT s,e FROM ({leaf}) unordered) d"
        )
        root_sum = sum(i * comb(17 - i, 6) for i in range(12))
        mixed_sql = call("/sql", {"statement": mixed_statement})
        assert mixed_sql["rows"][0][:3] == [
            str(2 * (root_sum + 31824)),
            "1989000",
            "63648",
        ], mixed_sql
        # Exercise the same negotiated cursor behind the pgwire authority guard.
        pgwire = subprocess.run(
            [
                "/opt/homebrew/opt/postgresql@18/bin/psql",
                f"host=127.0.0.1 port={server.pgwire_port} user=admin dbname=default sslmode=disable",
                "-X",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=mixed_statement
            + ";"
            + f"SELECT q.score FROM GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q "
            + "ORDER BY q.score DESC LIMIT 5 OFFSET 100;",
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
            env={**os.environ, "PGPASSWORD": AUTH_BOOTSTRAP_PASSWORD},
        )
        assert pgwire.returncode == 0, pgwire.stderr
        assert pgwire.stdout.strip().split("|")[:3] == [
            str(2 * (root_sum + 31824)),
            "1989000",
            "63648",
        ]
        assert pgwire.stdout.strip().splitlines()[1:] == ["17"] * 5, pgwire.stdout
        distinct_float = call(
            "/sql",
            {
                "statement": f"SELECT sum(DISTINCT CAST(d.score AS DOUBLE PRECISION)),"
                f"avg(DISTINCT CAST(d.score AS DOUBLE PRECISION)) FROM "
                f"(SELECT q.score AS score FROM GRAPH_TABLE({hops} MATCH {path} "
                "COLUMNS(b.score AS score)) q) d"
            },
        )
        assert distinct_float["rows"][0] == pytest.approx([138, 11.5], rel=1e-14), (
            distinct_float
        )
        for tail, expected in (
            ("WHERE a.score>=0 RETURN sum(b.score)", ["497250"]),
            ("WHERE b.score>=0 RETURN sum(b.score)", ["497250"]),
            ("WITH b ORDER BY b.score RETURN count(*),min(b.score)", ["31824", "6"]),
            (
                "WITH b ORDER BY b.score DESC SKIP 100 LIMIT 25 RETURN count(*),sum(b.score)",
                ["25", "425"],
            ),
        ):
            optimized = call(
                f"/graphs/{hops}/query",
                {
                    "query": "MATCH (a:H)-[:R*6..6]->(b:H) " + tail,
                    "limits": {"explored_edges": 153, "intermediate_states": 30000},
                },
            )
            assert [cell["value"] for cell in optimized["rows"][0]] == expected, (
                optimized
            )
        for component in (
            "MATCH (a:H)-[:R*3..3]->(m:H),(m)-[:R*3..3]->(b:H)",
            "MATCH (a:H)-[:R*3..3]->(m:H) MATCH (m)-[:R*3..3]->(b:H)",
        ):
            reduced = call(
                f"/graphs/{hops}/query",
                {
                    "query": component + " RETURN count(*),sum(b.score)",
                    "limits": {"explored_edges": 153, "intermediate_states": 30000},
                },
            )
            assert [cell["value"] for cell in reduced["rows"][0]] == [
                "31824",
                "497250",
            ], reduced
        branch_paths = sum(
            comb(m, 3) * comb(17 - m, 3) * (16 - m) for m in range(3, 15)
        )
        branch_sum = sum(
            comb(m, 3) * comb(b - m - 1, 2) * b * (16 - m)
            for m in range(3, 15)
            for b in range(m + 3, 18)
        )
        branched = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*3..3]->(m:H),(m)-[:R*3..3]->(b:H),(m)-[:R]->(c:H) RETURN count(*),sum(b.score)",
                "limits": {"explored_edges": 153, "intermediate_states": 30000},
            },
        )
        assert [cell["value"] for cell in branched["rows"][0]] == [
            str(branch_paths),
            str(branch_sum),
        ], branched
        many_branches = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*3..3]->(m:H),(m)-[:R*3..3]->(b:H)"
                + "".join(f",(m)-[:R]->(c{i}:H)" for i in range(7))
                + " RETURN count(*)",
                "limits": {"explored_edges": 153, "intermediate_states": 30000},
            },
        )
        assert many_branches["rows"] == [
            [
                {
                    "type": "integer",
                    "value": str(
                        sum(
                            comb(m, 3) * comb(17 - m, 3) * perm(16 - m, 7)
                            for m in range(3, 15)
                        )
                    ),
                }
            ]
        ], many_branches
        split_path = (
            "(a IS H)-[r0 IS R]->(u0 IS H)-[r1 IS R]->(u1 IS H)-[r2 IS R]->(m IS H),"
            "(m)-[r3 IS R]->(v0 IS H)-[r4 IS R]->(v1 IS H)-[r5 IS R]->(b IS H)"
        )
        relation = f"GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q"
        for statement, expected in (
            (
                f"SELECT sum(q.score) FROM GRAPH_TABLE({hops} MATCH {split_path} COLUMNS(b.score AS score)) q",
                [["497250"]],
            ),
            (
                f"SELECT q.score FROM {relation} ORDER BY q.score DESC LIMIT 5 OFFSET 100",
                [["17"]] * 5,
            ),
            (
                f"SELECT sum(d.score) FROM (SELECT q.score FROM {relation} ORDER BY q.score DESC LIMIT 5 OFFSET 100) d",
                [["85"]],
            ),
            (
                f"SELECT sum(q.score),count(*) FROM {relation} CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 AS k) d",
                [["994500", "63648"]],
            ),
            (
                f"SELECT count(*),sum(q.score) FROM {relation} JOIN (SELECT 16 AS k UNION ALL SELECT 17 AS k) d ON q.score=d.k",
                [["20384", "338520"]],
            ),
            (
                f"SELECT count(*),sum(q.score),sum(d.k) FROM {relation} LEFT JOIN (SELECT 16 AS k UNION ALL SELECT 17 AS k) d ON q.score=d.k",
                [["31824", "497250", "338520"]],
            ),
        ):
            compressed_sql = call("/sql", {"statement": statement})
            assert compressed_sql["rows"] == expected, compressed_sql
        selective_range = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) WHERE b.score>=17 AND b.score<18 RETURN count(*),sum(b.score)",
                "limits": {
                    "scanned_anchors": 10,
                    "explored_edges": 153,
                    "intermediate_states": 30000,
                },
            },
        )
        assert [cell["value"] for cell in selective_range["rows"][0]] == [
            "12376",
            "210392",
        ], selective_range
        assert selective_range["stats"]["scanned_anchors"] == 1, selective_range
        optional = call(
            f"/graphs/{hops}/query",
            {
                "query": "OPTIONAL MATCH (a:H)-[:R*6..6]->(b:H) RETURN count(*),sum(b.score),min(b.score)",
                "limits": {"explored_edges": 153, "intermediate_states": 20000},
            },
        )
        assert [cell["value"] for cell in optional["rows"][0]] == [
            "31824",
            "497250",
            "6",
        ], optional
        start = time.monotonic()
        computed_sql = call(
            "/sql",
            {
                "statement": f"SELECT sum(CASE WHEN q.score>0 THEN q.score ELSE 1/(q.score-q.score) END),"
                f"min(abs(q.score)),max(coalesce(q.score,0)),sum(DISTINCT abs(q.score)) "
                f"FROM GRAPH_TABLE({hops} MATCH {path} COLUMNS(b.score AS score)) q "
                f"WHERE q.score>0 AND (q.score<18 OR q.score IS NULL)"
            },
        )
        assert computed_sql["rows"] == [["497250", "6", "17", "138"]], computed_sql
        assert time.monotonic() - start < 10
        distinct_endpoints = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) RETURN DISTINCT b.score ORDER BY b.score",
                "limits": {"explored_edges": 1000, "intermediate_states": 30000},
            },
        )
        assert distinct_endpoints["rows"] == [
            [{"type": "integer", "value": str(i)}] for i in range(6, 18)
        ], distinct_endpoints
        pure_endpoint = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*6..6]->(b:H) RETURN min(CASE WHEN b.score>0 THEN b.score ELSE 1/0 END)",
                "limits": {"explored_edges": 153, "intermediate_states": 20000},
            },
        )
        assert pure_endpoint["rows"] == [[{"type": "integer", "value": "6"}]], (
            pure_endpoint
        )
        for tail, expected in (
            ("WITH b RETURN count(*),sum(b.score)", ["31824", "497250"]),
            ("WITH a,b RETURN count(*),sum(b.score)", ["31824", "497250"]),
            ("WITH a.id AS dead,b RETURN count(*),sum(b.score)", ["31824", "497250"]),
            ("WITH b.score AS score RETURN count(*),sum(score)", ["31824", "497250"]),
            ("WITH DISTINCT b RETURN count(*),sum(b.score)", ["12", "138"]),
            (
                "UNWIND [b.score,b.score] AS score RETURN count(*),sum(score)",
                ["63648", "994500"],
            ),
        ):
            pipeline = call(
                f"/graphs/{hops}/query",
                {
                    "query": "MATCH (a:H)-[:R*6..6]->(b:H) " + tail,
                    "limits": {"explored_edges": 153, "intermediate_states": 20000},
                },
            )
            assert [cell["value"] for cell in pipeline["rows"][0]] == expected, pipeline
        singleton = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H {score:0})-[:R*1..18]->(b:H) RETURN min(b.score),max(b.score)",
                "limits": {"explored_edges": 153, "intermediate_states": 10000},
            },
            wait_for_index=True,
        )
        assert [cell["value"] for cell in singleton["rows"][0]] == ["1", "17"], (
            singleton
        )
        for bounds, expected in (
            ("1..", 2**18 - 19),
            ("2..", 2**18 - 19 - 153),
            ("0..", 2**18 - 1),
        ):
            all_lengths = call(
                f"/graphs/{hops}/query",
                {
                    "query": f"MATCH (a:H)-[:R*{bounds}]->(b:H) RETURN count(*)",
                    "limits": {"explored_edges": 153, "intermediate_states": 20000},
                },
            )
            assert all_lengths["rows"] == [
                [{"type": "integer", "value": str(expected)}]
            ], all_lengths
        start_only = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*1..]->(b:H) RETURN sum(a.score)",
                "limits": {"explored_edges": 153, "intermediate_states": 20000},
            },
        )
        expected_start_sum = sum(i * (2 ** (17 - i) - 1) for i in range(18))
        assert start_only["rows"] == [
            [{"type": "integer", "value": str(expected_start_sum)}]
        ], start_only
        mixed = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H)-[:R*1..]->(b:H) RETURN sum(a.score),sum(b.score),count(*)",
                "limits": {"explored_edges": 306, "intermediate_states": 20000},
            },
        )
        assert [cell["value"] for cell in mixed["rows"][0]] == [
            str(expected_start_sum),
            str(sum(i * (2**i - 1) for i in range(18))),
            str(2**18 - 19),
        ], mixed
        pure_columns = call(
            "/sql",
            {
                "statement": f"SELECT min(CAST(q.score AS BIGINT)) FROM GRAPH_TABLE({hops} MATCH {path} "
                "COLUMNS(CASE WHEN b.score>0 THEN b.score ELSE 1/0 END AS score)) q"
            },
        )
        assert pure_columns["rows"] == [["6"]], pure_columns
        phase = call(
            f"/graphs/{hops}/query",
            {
                "query": "CREATE (:H {id:'read_phase_marker'}) WITH 1 AS ignored "
                "MATCH (a:H)-[:R*6..6]->(b:H) RETURN count(*)",
                "limits": {"intermediate_states": 100000, "explored_edges": 5000},
            },
        )
        assert phase["rows"] == [[{"type": "integer", "value": "31824"}]], phase
        comma = call(
            "/sql",
            {
                "statement": f"SELECT q.first,q.second FROM GRAPH_TABLE({graph} MATCH (a IS D),(b IS D) COLUMNS(a.score AS first,b.score AS second)) q LIMIT 1"
            },
        )
        assert len(comma["rows"]) == 1, comma
        expanded = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) UNWIND [1,2] AS x RETURN n.score,x LIMIT 1",
                "limits": {"scanned_anchors": 1},
            },
        )
        assert len(expanded["rows"]) == 1, expanded
        sought = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:D) WHERE n.score=1998 OR n.score=1999 RETURN n.score ORDER BY n.score",
                "limits": {"scanned_anchors": 2},
            },
            wait_for_index=True,
        )
        assert sought["rows"] == [
            [{"type": "integer", "value": "1998"}],
            [{"type": "integer", "value": "1999"}],
        ], sought
        call(
            f"/graphs/{graph}_rare",
            {
                "labels": [{"name": "Rare", "table": table, "where": {"score": 1999}}],
                "relationships": [],
            },
        )
        rare = call(
            f"/graphs/{graph}_rare/query",
            {
                "query": "MATCH (n:Rare) RETURN count(*)",
                "limits": {"scanned_anchors": 1},
            },
            wait_for_index=True,
        )
        assert rare["rows"] == [[{"type": "integer", "value": "1"}]], rare
        merged = call(
            f"/graphs/{graph}/query",
            {
                "query": "UNWIND $ids AS id MERGE (n:D {id:id}) RETURN count(*)",
                "parameters": {"ids": [f"new{i}" for i in range(800)]},
                "limits": {"scanned_anchors": 0, "intermediate_states": 200000},
            },
        )
        assert merged["rows"] == [[{"type": "integer", "value": "800"}]], merged
        payload_table = table + "_document_payload"
        call("/tables/" + payload_table, {"num_shards": 1})
        for start in range(0, 200, 50):
            call(
                f"/tables/{payload_table}/batch",
                {
                    "inserts": {
                        f"n{i}": {
                            "body": "needle",
                            "payload": "x" * 8192,
                            "literal.field": 7,
                        }
                        for i in range(start, start + 50)
                    },
                    "sync_level": "full_index",
                },
            )
        call(
            "/graphs/" + payload_table,
            {"labels": [{"name": "P", "table": payload_table}], "relationships": []},
        )
        for query in (
            "MATCH (n:P) RETURN count(*)",
            f"CALL antfly.search('{payload_table}',{{query:'body:needle',mode:'text',limit:200}}) YIELD score RETURN count(*)",
        ):
            pruned = call(
                f"/graphs/{payload_table}/query",
                {"query": query, "limits": {"memory_bytes": 8 << 20}},
            )
            assert pruned["rows"] == [[{"type": "integer", "value": "200"}]], pruned
        for expression, expected in (
            ("min(n.body)", {"type": "string", "value": "needle"}),
            ("min(n.`literal.field`)", {"type": "integer", "value": "7"}),
        ):
            projected = call(
                f"/graphs/{payload_table}/query",
                {"query": f"MATCH (n:P) RETURN {expression}"},
            )
            assert projected["rows"] == [[expected]], projected
        call("/sql", {"statement": f"CREATE INDEX {hops}_id ON {hops}(id)"})
        related = call(
            f"/graphs/{hops}/query",
            {
                "query": "MATCH (a:H {id:'n0'}) UNWIND $ids AS id MERGE (b:H {id:id}) MERGE (a)-[:R]->(b) RETURN count(*)",
                "parameters": {"ids": [f"fresh{i}" for i in range(800)]},
            },
        )
        assert related["rows"] == [[{"type": "integer", "value": "800"}]], related
        assert related["stats"]["explored_edges"] < 1000, related
        for pattern in ("(a)-[:R]->(b)", "(b)<-[:R]-(a)"):
            pair = call(
                f"/graphs/{hops}/query",
                {
                    "query": f"MATCH (a:H {{id:'n0'}}),(b:H {{id:'fresh799'}}) MATCH {pattern} RETURN count(*)",
                    "limits": {"explored_edges": 10},
                },
                wait_for_index=True,
            )
            assert pair["rows"] == [[{"type": "integer", "value": "1"}]], pair
            assert pair["stats"]["explored_edges"] <= 2, pair
        # Exercise presence negotiation through the authenticated SQL cursor
        # wrappers with more paths than fit an exact signed multiplicity.
        layers = 64
        patterns = ["(s:H {id:'presence_start',score:0})"] + [
            f"(v{i}_{j}:H {{score:{i + 1}}})" for i in range(layers) for j in range(2)
        ]
        patterns += [f"(s)-[:R]->(v0_{j})" for j in range(2)]
        patterns += [
            f"(v{i}_{j})-[:R]->(v{i + 1}_{k})"
            for i in range(layers - 1)
            for j in range(2)
            for k in range(2)
        ]
        call(f"/graphs/{hops}/query", {"query": "CREATE " + ",".join(patterns)})
        presence_path = (
            "(a IS H)"
            + "".join(f"-[r{i} IS R]->(m{i} IS H)" for i in range(63))
            + "-[r63 IS R]->(b IS H)"
        )
        presence_sql = call(
            "/sql",
            {
                "statement": "SELECT min(q.score),max(q.score),count(DISTINCT q.score) "
                f"FROM GRAPH_TABLE({hops} MATCH {presence_path} "
                "WHERE a.id='presence_start' COLUMNS(b.score AS score)) q"
            },
        )
        assert presence_sql["rows"] == [["64", "64", "1"]], presence_sql
        saturated_top = call(
            "/sql",
            {
                "statement": f"SELECT q.score FROM GRAPH_TABLE({hops} MATCH {presence_path} "
                "WHERE a.id='presence_start' COLUMNS(b.score AS score)) q "
                "CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 AS k) d "
                "ORDER BY q.score DESC LIMIT 5 OFFSET 1000"
            },
        )
        assert saturated_top["rows"] == [["64"]] * 5, saturated_top
        presence_relation = (
            f"GRAPH_TABLE({hops} MATCH {presence_path} "
            "WHERE a.id='presence_start' COLUMNS(b.score AS score)) q"
        )
        for order in ("", "ORDER BY q.score DESC"):
            window = f"SELECT q.score FROM {presence_relation} {order} LIMIT 5000000 OFFSET 1000"
            reduced_window = call(
                "/sql",
                {"statement": f"SELECT count(*),sum(d.score) FROM ({window}) d"},
            )
            assert reduced_window["rows"] == [["5000000", "320000000"]], reduced_window
            counted_window = call(
                "/sql", {"statement": f"SELECT count(*) FROM ({window}) d"}
            )
            assert counted_window["rows"] == [["5000000"]], counted_window
        for order in ("", "ORDER BY b.score DESC"):
            reduced_window = call(
                f"/graphs/{hops}/query",
                {
                    "query": "MATCH (s:H {id:'presence_start'})-[:R*64..64]->(b:H) "
                    f"WITH b {order} SKIP 1000 LIMIT 5000000 RETURN count(*),sum(b.score)",
                    "limits": {"intermediate_states": 30000, "explored_edges": 300},
                },
            )
            assert [cell["value"] for cell in reduced_window["rows"][0]] == [
                "5000000",
                "320000000",
            ], reduced_window
        script = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "verify_cypher_sql_transaction.py"
        )
        for policy in (False, True):
            command = [
                sys.executable,
                str(script),
                "--url",
                server.url,
                "--pgport",
                str(server.pgwire_port),
            ]
            if policy:
                command.append("--policy")
            result = subprocess.run(
                command,
                capture_output=True,
                check=False,
                text=True,
                timeout=220,
                env={
                    **os.environ,
                    "ANTFLY_CYPHER_TEST_AUTH": f"admin:{AUTH_BOOTSTRAP_PASSWORD}",
                },
            )
            assert result.returncode == 0, (
                result.stdout + result.stderr + server.log_path.read_text()[-6000:]
            )
    finally:
        session.close()
        server.stop()
