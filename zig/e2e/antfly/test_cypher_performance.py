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
        config.setdefault("resources", {}).setdefault("query", {}).setdefault(
            "request_limits", {}
        )["graph"] = {"max_intermediate_states": 1000000}
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
                    and (
                        response.status_code == 429
                        and response.json().get("code") == "CypherAnchorBudgetExceeded"
                        or response.status_code == 503
                        and response.json().get("code") == "Backpressured"
                    )
                    and time.monotonic() < deadline
                ):
                    break
                # Publication precedes asynchronous backfill, whose metadata
                # commit can also hold coherent-read admission briefly. Retry
                # only readiness probes until their strict budget proves READY.
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
        # Plan the complete predicate before selecting an IN transport window.
        # The broad score domain must not hide the selective id access path.
        for predicate, expected, anchors in (
            ("n.score IN range(0,1999) AND n.id='n1999'", 1, 1),
            ("n.score IN range(0,1999) AND n.id IN ['n1999','n1998']", 2, 2),
            ("n.id IN ['n1999','n1998'] AND n.score IN range(0,1999)", 2, 2),
            ("n.absent IN [1,2] AND n.id='n1999'", 0, 1),
            ("n.id IN []", 0, 1),
            ("n.id IN [null]", 0, 1),
        ):
            planned = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"MATCH(n:D) WHERE {predicate} RETURN count(*)",
                    "limits": {"scanned_anchors": anchors},
                },
            )
            assert planned["rows"] == [[{"type": "integer", "value": str(expected)}]], (
                planned
            )
            assert planned["stats"]["scanned_anchors"] <= anchors, planned
        # Membership must stay on bounded indexed seeks across the old 256
        # cutoff; physical work must depend on the domain, not table size.
        for size in (128, 256, 257, 400):
            membership = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"MATCH(n:D) WHERE n.id IN [i IN range(0,{size - 1}) | 'n'+toString(i)] RETURN sum(n.score)",
                    "limits": {"scanned_anchors": size, "intermediate_states": 50000},
                },
            )
            assert membership["rows"] == [
                [{"type": "integer", "value": str(size * (size - 1) // 2)}]
            ], membership
            assert membership["stats"]["scanned_anchors"] <= size, membership
        incompatible = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH(n:D) WHERE n.id IN range(0,399) RETURN count(*)",
                "limits": {"scanned_anchors": 2000, "intermediate_states": 100000},
            },
        )
        assert incompatible["rows"] == [[{"type": "integer", "value": "0"}]], (
            incompatible
        )
        assert incompatible["stats"]["scanned_anchors"] <= 2000, incompatible
        nonunique = table + "_nonunique"
        call("/sql", {"statement": f"CREATE TABLE {nonunique} (bucket BIGINT)"})
        call(
            "/sql",
            {"statement": f"CREATE INDEX {nonunique}_bucket ON {nonunique}(bucket)"},
        )
        call(
            f"/tables/{nonunique}/batch",
            {
                "inserts": {f"n{i}": {"bucket": i // 2} for i in range(254)},
                "sync_level": "full_index",
            },
        )
        call(
            f"/graphs/{nonunique}",
            {"labels": [{"name": "U", "table": nonunique}], "relationships": []},
        )
        call(
            f"/graphs/{nonunique}/query",
            {
                "query": "MATCH(n:U {bucket:0}) RETURN n.bucket",
                "limits": {"scanned_anchors": 2},
            },
            wait_for_index=True,
        )
        adaptive = call(
            f"/graphs/{nonunique}/query",
            {
                "query": "UNWIND range(0,126) AS k MATCH(n:U) WHERE n.bucket=k RETURN count(*)",
                "limits": {"scanned_anchors": 254},
            },
        )
        assert adaptive["rows"] == [[{"type": "integer", "value": "254"}]], adaptive
        assert adaptive["stats"]["scanned_anchors"] == 254, adaptive
        # Only a composite index exists: each complete (tenant,id) prefix
        # must avoid the 1,000-row scan of the two tenant-only prefixes.
        composite = table + "_composite"
        call("/sql", {"statement": f"CREATE TABLE {composite} (tenant BIGINT,id TEXT)"})
        call(
            "/sql",
            {"statement": f"CREATE INDEX {composite}_keys ON {composite}(tenant,id)"},
        )
        call(
            f"/tables/{composite}/batch",
            {
                "inserts": {
                    f"n{i}": {"tenant": i % 4, "id": f"n{i}"} for i in range(2000)
                },
                "sync_level": "full_index",
            },
        )
        call(
            f"/graphs/{composite}",
            {"labels": [{"name": "C", "table": composite}], "relationships": []},
        )
        call(
            f"/graphs/{composite}/query",
            {
                "query": "MATCH(n:C {tenant:1,id:'n1997'}) RETURN n.id",
                "limits": {"scanned_anchors": 1},
            },
            wait_for_index=True,
        )
        for predicate in (
            "n.tenant IN [1,2] AND n.id IN ['n1997','n1998']",
            "n.id IN ['n1997','n1998'] AND n.tenant IN [1,2]",
            "n.tenant IN [1,1,null,2] AND n.id IN ['n1997','n1998','n1997']",
        ):
            precise_composite = call(
                f"/graphs/{composite}/query",
                {
                    "query": f"MATCH(n:C) WHERE {predicate} RETURN count(*)",
                    "limits": {"scanned_anchors": 2},
                },
            )
            assert precise_composite["rows"] == [[{"type": "integer", "value": "2"}]], (
                precise_composite
            )
            assert precise_composite["stats"]["scanned_anchors"] == 2, precise_composite
        # Public requests remain small while derived owner domains exceed the
        # private packet ceiling. Both matches are in the last value window.
        for length, domains in ((3000, 2), (12000, 1)):
            strings = table + f"_strings_{length}"
            prefix = "x" * length
            call("/sql", {"statement": f"CREATE TABLE {strings} (id TEXT,other TEXT)"})
            call("/sql", {"statement": f"CREATE INDEX {strings}_id ON {strings}(id)"})
            call(
                "/sql",
                {"statement": f"CREATE INDEX {strings}_other ON {strings}(other)"},
            )
            call(
                f"/tables/{strings}/batch",
                {
                    "inserts": {
                        f"n{i}": {"id": prefix + str(i), "other": prefix + str(i)}
                        for i in (398, 399)
                    },
                    "sync_level": "full_index",
                },
            )
            call(
                f"/graphs/{strings}",
                {"labels": [{"name": "S", "table": strings}], "relationships": []},
            )
            call(
                f"/graphs/{strings}/query",
                {
                    "query": "MATCH(n:S {id:$key}) RETURN n.id",
                    "parameters": {"key": prefix + "399"},
                    "limits": {"scanned_anchors": 1},
                },
                wait_for_index=True,
            )
            collection = "[i IN range(0,399) | $prefix+toString(i)]"
            predicate = f"n.id IN {collection}"
            if domains == 2:
                predicate += f" AND n.other IN {collection}"
            long_membership = call(
                f"/graphs/{strings}/query",
                {
                    "query": f"MATCH(n:S) WHERE {predicate} RETURN count(*)",
                    "parameters": {"prefix": prefix},
                    "limits": {"scanned_anchors": 2, "intermediate_states": 1000000},
                },
            )
            assert long_membership["rows"] == [[{"type": "integer", "value": "2"}]], (
                long_membership
            )
            assert long_membership["stats"]["scanned_anchors"] == 2, long_membership
        # A later out-of-range integer must invalidate DOUBLE seek coercion
        # for the whole domain before any earlier window can emit candidates.
        floating = table + "_floating"
        call("/sql", {"statement": f"CREATE TABLE {floating} (value DOUBLE PRECISION)"})
        call(
            "/sql", {"statement": f"CREATE INDEX {floating}_value ON {floating}(value)"}
        )
        call(
            f"/tables/{floating}/batch",
            {
                "inserts": {
                    **{f"n{i}": {"value": float(i)} for i in range(20)},
                    "rounded": {"value": 9007199254740992.0},
                    "exact_large": {"value": 9007199254740994.0},
                },
                "sync_level": "full_index",
            },
        )
        call(
            f"/graphs/{floating}",
            {"labels": [{"name": "F", "table": floating}], "relationships": []},
        )
        call(
            f"/graphs/{floating}/query",
            {
                "query": "MATCH(n:F {value:19}) RETURN n.value",
                "limits": {"scanned_anchors": 1},
            },
            wait_for_index=True,
        )
        for predicate, expected in (
            ("n.value=9007199254740994", 1),
            ("n.absent IN [1,2] AND n.value=9007199254740994", 0),
        ):
            exact_large = call(
                f"/graphs/{floating}/query",
                {
                    "query": f"MATCH(n:F) WHERE {predicate} RETURN count(*)",
                    "limits": {"scanned_anchors": 1},
                },
            )
            assert exact_large["rows"] == [
                [{"type": "integer", "value": str(expected)}]
            ], exact_large
            assert exact_large["stats"]["scanned_anchors"] == 1, exact_large
        precise = call(
            f"/graphs/{floating}/query",
            {
                "query": "MATCH(n:F) WHERE n.value IN $values RETURN count(*)",
                "parameters": {"values": [*range(256), 9007199254740993]},
                "limits": {"scanned_anchors": 22, "intermediate_states": 50000},
            },
        )
        assert precise["rows"] == [[{"type": "integer", "value": "20"}]], precise
        # Row-bound computed document keys must use owner point reads, with
        # the complete Cypher predicate still applied to each returned node.
        for key_predicate in (
            "antfly.documentKey(n)='n'+toString(i)",
            "antfly.documentKey(n) IN ['n'+toString(i)]",
            "antfly.documentKey(n)='n'+toString(coalesce(i,0))",
            "n.id='n'+toString(i)",
            "n.id IN [k IN [i] | 'n'+toString(k)]",
        ):
            computed = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"UNWIND range(0,399) AS i MATCH(n:D) WHERE {key_predicate} RETURN sum(n.score)",
                    "limits": {"scanned_anchors": 400, "intermediate_states": 50000},
                },
            )
            assert computed["rows"] == [[{"type": "integer", "value": "79800"}]], (
                computed
            )
            assert computed["stats"]["scanned_anchors"] <= 400, computed
        computed = call(
            f"/graphs/{graph}/query",
            {
                "query": "UNWIND range(0,399) AS i MATCH(n:D {id:'n'+toString(i)}) RETURN sum(n.score)",
                "limits": {"scanned_anchors": 400, "intermediate_states": 50000},
            },
        )
        assert computed["rows"] == [[{"type": "integer", "value": "79800"}]], computed
        assert computed["stats"]["scanned_anchors"] <= 400, computed
        for collection in (
            "[i IN range(0,399) | 'n'+toString(i)]",
            "coalesce([i IN range(0,399) | 'n'+toString(i)], [])",
            "reverse([i IN range(0,399) | 'n'+toString(i)])",
        ):
            computed = call(
                f"/graphs/{graph}/query",
                {
                    "query": f"MATCH(n:D) WHERE antfly.documentKey(n) IN {collection} RETURN sum(n.score)",
                    "limits": {"scanned_anchors": 400, "intermediate_states": 50000},
                },
            )
            assert computed["rows"] == [[{"type": "integer", "value": "79800"}]], (
                computed
            )
            assert computed["stats"]["scanned_anchors"] <= 400, computed
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
            "MATCH (n:P) RETURN count(n)",
            "MATCH (n:P) RETURN count(n.body)",
            "MATCH (n:P) RETURN count(n.body)+count(n.missing)",
            "MATCH (n:P) WHERE antfly.documentKey(n) IN [i IN range(0,199) | 'n'+toString(i)] RETURN count(n.body)",
            f"CALL antfly.search('{payload_table}',{{query:'body:needle',mode:'text',limit:200}}) YIELD score RETURN count(*)",
        ):
            pruned = call(
                f"/graphs/{payload_table}/query",
                {"query": query, "limits": {"memory_bytes": 8 << 20}},
            )
            assert pruned["rows"] == [[{"type": "integer", "value": "200"}]], pruned
        vector_count = call(
            f"/graphs/{payload_table}/query",
            {
                "query": "MATCH (n:P) RETURN count(*),count(n.body),count(n.missing)",
                "limits": {"memory_bytes": 8 << 20},
            },
        )
        assert vector_count["rows"] == [
            [
                {"type": "integer", "value": "200"},
                {"type": "integer", "value": "200"},
                {"type": "integer", "value": "0"},
            ]
        ], vector_count
        empty_count = call(
            f"/graphs/{payload_table}/query",
            {"query": "MATCH (n:P {body:'absent'}) RETURN count(n)"},
        )
        assert empty_count["rows"] == [[{"type": "integer", "value": "0"}]], empty_count
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
        # More keys than the SQL hash-group budget: every wrapper must forward
        # ordered group pulls, including selective HAVING and explicit NULL order.
        ordered_table = table + "_ordered"
        call(
            "/sql",
            {"statement": f"CREATE TABLE {ordered_table} (id TEXT, score BIGINT)"},
        )
        call(
            "/sql",
            {
                "statement": f"CREATE INDEX {ordered_table}_keys ON {ordered_table}(id,score DESC)"
            },
        )
        call(
            "/sql",
            {
                "statement": f"CREATE INDEX {ordered_table}_partition ON {ordered_table}(score,id)"
            },
        )
        # This fixture tests ordered pulls, not large asynchronous backfills.
        # Establish READY on the empty table before bulk writes maintain both
        # indexes; fast statement cuts deliberately do not drain build jobs.
        ready_until = time.monotonic() + 60
        while True:
            response = session.get(
                server.api_url.rstrip("/") + f"/tables/{ordered_table}/indexes",
                timeout=45,
            )
            assert response.status_code == 200, response.text
            indexes = response.json()
            states = [index["status"]["relational_index"]["state"] for index in indexes]
            assert "failed" not in states, indexes
            if len(states) == 2 and all(state == "ready" for state in states):
                break
            assert time.monotonic() < ready_until, indexes
            time.sleep(0.1)
        for start in range(0, 64000, 4000):
            call(
                f"/tables/{ordered_table}/batch",
                {
                    "inserts": {
                        f"k{i:06}": {"id": f"k{i:06}", "score": i % 3}
                        for i in range(start, start + 4000)
                    },
                    "sync_level": "full_index",
                },
            )
        call(
            f"/graphs/{ordered_table}",
            {"labels": [{"name": "D", "table": ordered_table}], "relationships": []},
        )
        call(
            f"/graphs/{ordered_table}/query",
            {
                "query": "MATCH(n:D) RETURN n.id ORDER BY n.id LIMIT 1",
                "limits": {"scanned_anchors": 4},
            },
            wait_for_index=True,
        )
        for width in (9, 32):
            partitioned = call(
                f"/graphs/{ordered_table}/query",
                {
                    "query": "MATCH(n:D) WHERE n.score IN $values RETURN n.id ORDER BY n.id LIMIT 10",
                    "parameters": {"values": [0, 1, 2, *range(999, 999 + width - 3)]},
                    "limits": {"scanned_anchors": 64},
                },
                wait_for_index=True,
            )
            assert [row[0]["value"] for row in partitioned["rows"]] == [
                f"k{i:06}" for i in range(10)
            ]
            assert partitioned["stats"]["scanned_anchors"] <= 16
        ordered_source = f"GRAPH_TABLE({ordered_table} MATCH(n IS D) COLUMNS(n.id AS id,n.score AS score)) q"
        ordered_queries = []
        expected_groups = [[f"k{i:06}", "1"] for i in (7, 10, 13, 16, 19)]
        for observer in (
            "count(DISTINCT score)+0",
            "coalesce(count(DISTINCT score),0)",
        ):
            statement = (
                f"SELECT id,{observer} FROM {ordered_source} GROUP BY id "
                "HAVING min(score)=1 ORDER BY id ASC NULLS FIRST OFFSET 2 LIMIT 5"
            )
            assert call("/sql", {"statement": statement})["rows"] == expected_groups
            ordered_queries.append(statement)
        pg_expected = ["|".join(row) for row in expected_groups] * 2
        for having, expected in (
            ("id='k063999' AND count(DISTINCT score)>0", [["k063999", "1"]]),
            ("false", []),
        ):
            statement = (
                f"SELECT id,count(DISTINCT score) FROM {ordered_source} GROUP BY id "
                f"HAVING {having} ORDER BY id ASC NULLS FIRST LIMIT 1"
            )
            assert call("/sql", {"statement": statement})["rows"] == expected
            ordered_queries.append(statement)
            pg_expected.extend("|".join(row) for row in expected)
        parameter_source = ordered_source.replace(
            "MATCH(n IS D)", "MATCH(n IS D) WHERE n.score IN $1"
        )
        assert call(
            "/sql",
            {
                "statement": f"SELECT id,count(DISTINCT score) FROM {parameter_source} GROUP BY id ORDER BY id OFFSET 999 LIMIT 1",
                "parameters": [[0, 1, 2] * 100],
            },
        )["rows"] == [["k000999", "1"]]
        pg_groups = subprocess.run(
            [
                "/opt/homebrew/opt/postgresql@18/bin/psql",
                f"host=127.0.0.1 port={server.pgwire_port} user=admin dbname=default sslmode=disable",
                "-X",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=";".join(ordered_queries) + ";",
            capture_output=True,
            text=True,
            timeout=45,
            env={**os.environ, "PGPASSWORD": AUTH_BOOTSTRAP_PASSWORD},
        )
        assert pg_groups.returncode == 0, pg_groups.stderr
        assert pg_groups.stdout.strip().splitlines() == pg_expected
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


def test_authenticated_cypher_producer_visibility_uses_bounded_scratch():
    """One wide producer must not be retained once per relationship or filter."""
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    prefix = "cypher_visibility_" + uuid.uuid4().hex
    admin = requests.Session()
    admin.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    reader = requests.Session()
    reader.auth = (prefix + "_reader", uuid.uuid4().hex)
    base = server.api_url.rstrip("/")

    def call(path, body=None, *, method="POST", session=admin, read_only=False):
        deadline = time.monotonic() + 60
        while True:
            response = session.request(
                method,
                (server.url if path.startswith("/auth/") else base) + path,
                json=body,
                timeout=45,
            )
            if not (
                read_only
                and response.status_code == 503
                and time.monotonic() < deadline
            ):
                break
            time.sleep(0.1)
        assert response.status_code in (200, 201, 202, 204), (
            path,
            response.status_code,
            response.text,
            server.log_path.read_text()[-4000:],
        )
        return response.json() if response.content else None

    def query(session, expected):
        result = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH (a:D)-[r:R]->(b:D) WHERE antfly.documentKey(a)='a' RETURN count(r)",
                "limits": {"memory_bytes": 8 << 20},
            },
            session=session,
            read_only=True,
        )
        assert result["complete"] is True, result
        assert result["rows"] == [[{"type": "integer", "value": str(expected)}]], result
        assert result["stats"]["memory_bytes"] < 8 << 20, result

    edges = [
        {
            "index_name": "links",
            "source": "a",
            "target": "b",
            "edge_type": "link",
            "edge_id": str(i),
            "owner_document": "p",
        }
        for i in range(128)
    ]
    try:
        call(
            f"/tables/{prefix}",
            {
                "num_shards": 1,
                "indexes": {
                    "links": {"type": "graph", "edge_types": [{"name": "link"}]}
                },
            },
        )
        deadline = time.monotonic() + 60
        while True:
            index = call(f"/tables/{prefix}/indexes/links", method="GET")
            complete = index.get("status", {}).get("milestones", {}).get("complete", {})
            if complete.get("reached") and not complete.get("blockers"):
                break
            assert time.monotonic() < deadline, index
            time.sleep(0.1)
        call(
            f"/graphs/{prefix}",
            {
                "labels": [{"name": "D", "table": prefix}],
                "relationships": [
                    {
                        "name": "R",
                        "index": "links",
                        "source_label": "D",
                        "target_label": "D",
                        "stored_type": "link",
                    }
                ],
            },
        )
        call(
            f"/tables/{prefix}/batch",
            {
                "inserts": {
                    "a": {"tenant": "acme"},
                    "b": {"tenant": "acme"},
                    "p": {"tenant": "acme", "payload": "x" * (1 << 20)},
                },
                "graph_writes": edges,
                "sync_level": "full_index",
            },
        )
        call(
            f"/auth/v1/users/{reader.auth[0]}",
            {
                "password": reader.auth[1],
                "initial_policies": [
                    {"resource_type": "graph", "resource": prefix, "type": "read"},
                    {"resource_type": "table", "resource": prefix, "type": "read"},
                ],
            },
        )
        call(
            f"/auth/v1/users/{reader.auth[0]}/row-filters/{prefix}",
            {"term": {"tenant": "acme"}},
            method="PUT",
        )
        query(admin, 128)
        query(reader, 128)
        # New statements cannot inherit either a positive or negative decision
        # from an older producer version, even with unchanged endpoints/edges.
        for tenant, expected in (("other", 0), ("acme", 128)):
            call(
                f"/tables/{prefix}/batch",
                {
                    "inserts": {"p": {"tenant": tenant, "payload": "x" * (1 << 20)}},
                    "graph_writes": edges,
                    "sync_level": "full_index",
                },
            )
            query(reader, expected)
    finally:
        reader.close()
        admin.close()
        server.stop()


@pytest.mark.parametrize("relational", [False, True], ids=["document", "relational"])
def test_authenticated_cypher_count_nullness_streams_wide_properties(relational):
    """COUNT keeps source payloads borrowed on both owner storage paths."""
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    prefix = "cypher_count_" + uuid.uuid4().hex
    admin = requests.Session()
    admin.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    reader = requests.Session()
    reader.auth = (prefix + "_reader", uuid.uuid4().hex)
    base = server.api_url.rstrip("/")

    def call(path, body=None, *, method="POST", session=admin, wait_for_index=False):
        deadline = time.monotonic() + 60
        while True:
            response = session.request(
                method,
                (server.url if path.startswith("/auth/") else base) + path,
                json=body,
                timeout=45,
            )
            waiting = response.status_code == 503 or (
                wait_for_index
                and response.status_code == 429
                and response.json().get("code") == "CypherAnchorBudgetExceeded"
            )
            if not waiting or time.monotonic() >= deadline:
                break
            time.sleep(0.1)
        assert response.status_code in (200, 201, 202, 204), (
            path,
            response.status_code,
            response.text,
            server.log_path.read_text()[-4000:],
        )
        return response.json() if response.content else None

    def count(label, session, expected):
        result = call(
            f"/graphs/{prefix}/query",
            {
                "query": f"MATCH (n:{label}) RETURN count(*),count(n),count(n.body),"
                "count(n.missing),count($p),count(n.body)+count(*)",
                "parameters": {"p": False},
                "limits": {"memory_bytes": 1 << 20},
            },
            session=session,
        )
        rows, bodies = expected
        assert result["complete"] is True, result
        assert result["rows"] == [
            [
                {"type": "integer", "value": str(value)}
                for value in (rows, rows, bodies, 0, rows, bodies + rows)
            ]
        ], result
        assert result["stats"]["memory_bytes"] < 1 << 20, result
        return result

    try:
        if relational:
            call(
                "/sql",
                {
                    "statement": f"CREATE TABLE {prefix} (id TEXT, tenant TEXT, body TEXT, enabled BOOLEAN)"
                },
            )
            call("/sql", {"statement": f"CREATE INDEX {prefix}_id ON {prefix}(id)"})
        else:
            call(f"/tables/{prefix}", {"num_shards": 1})
        for start in range(0, 32, 4):
            call(
                f"/tables/{prefix}/batch",
                {
                    "inserts": {
                        f"n{i}": {
                            "id": f"n{i}",
                            "enabled": i % 2 == 1,
                            "tenant": "acme" if i % 4 < 2 else "other",
                            "body": ('escaped\\"\n' + "x" * (512 << 10))
                            if i % 2
                            else None,
                        }
                        for i in range(start, start + 4)
                    },
                    "sync_level": "full_index",
                },
            )
        call(
            f"/graphs/{prefix}",
            {
                "labels": [
                    {"name": "D", "table": prefix},
                    {"name": "A", "table": prefix, "where": {"tenant": "acme"}},
                    {"name": "One", "table": prefix, "where": {"id": "n1"}},
                ],
                "relationships": [],
            },
        )
        count("D", admin, (32, 16))
        count("A", admin, (16, 8))
        # With no unconditional label, an unlabeled pattern is a union of
        # predicate memberships. Overlapping labels must not double count.
        call(
            f"/graphs/{prefix}_union",
            {
                "labels": [
                    {"name": "A", "table": prefix, "where": {"tenant": "acme"}},
                    {"name": "One", "table": prefix, "where": {"id": "n1"}},
                ],
                "relationships": [],
            },
        )
        union = call(
            f"/graphs/{prefix}_union/query",
            {
                "query": "MATCH(n) RETURN count(*),count(n.body)",
                "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 32},
            },
        )
        assert union["rows"] == [
            [{"type": "integer", "value": "16"}, {"type": "integer", "value": "8"}]
        ], union
        assert union["stats"]["scanned_anchors"] == 32, union

        if relational:
            indexed_label = call(
                f"/graphs/{prefix}/query",
                {
                    "query": "MATCH (n:One) RETURN count(n.body)",
                    "limits": {"memory_bytes": 8 << 20, "scanned_anchors": 1},
                },
                wait_for_index=True,
            )
            assert indexed_label["rows"] == [[{"type": "integer", "value": "1"}]], (
                indexed_label
            )
            assert indexed_label["stats"]["scanned_anchors"] == 1, indexed_label
        measured = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH (n:D) RETURN count(n.body)",
                "limits": {"memory_bytes": 8 << 20, "scanned_anchors": 32},
            },
        )
        assert measured["rows"] == [[{"type": "integer", "value": "16"}]], measured
        print(
            json.dumps(
                {
                    "count_storage": "relational" if relational else "document",
                    "stats": measured["stats"],
                },
                sort_keys=True,
            )
        )
        # A predicate on an unrelated label must not turn a count's private
        # membership projection into a full wide-value observation.
        wide_graph = prefix + "_wide_label"
        call(
            f"/graphs/{wide_graph}",
            {
                "labels": [
                    {"name": "D", "table": prefix},
                    {"name": "Wide", "table": prefix, "where": {"body": "unmatched"}},
                ],
                "relationships": [],
            },
        )
        for pattern, aggregate, expected in (
            ("n:D", "count(*)", 32),
            ("n:D", "count(n.body)", 16),
            ("n", "count(n.body)", 16),
        ):
            bounded = call(
                f"/graphs/{wide_graph}/query",
                {
                    "query": f"MATCH({pattern}) RETURN {aggregate}",
                    "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 32},
                },
            )
            assert bounded["rows"] == [[{"type": "integer", "value": str(expected)}]], (
                bounded
            )
            assert bounded["stats"]["memory_bytes"] < 1 << 20, bounded
            assert bounded["stats"]["scanned_anchors"] == 32, bounded
        # The same demand proof licenses ordinary expressions and aggregates,
        # including a catalog predicate that the query does not observe.
        for graph_name in (prefix, wide_graph):
            for query, expected in (
                ("MATCH(n:D) RETURN sum(1)", 32),
                (
                    "MATCH(n:D) RETURN sum(CASE WHEN n.body IS NOT NULL THEN 1 ELSE 0 END)",
                    16,
                ),
                ("MATCH(n:D) WHERE n.body IS NOT NULL RETURN sum(1)", 16),
                ("MATCH(n:D) WHERE n.body IS NULL RETURN sum(1)", 16),
                (
                    "MATCH(n:D) WITH n AS x RETURN sum(CASE WHEN x.body IS NULL THEN 0 ELSE 1 END)",
                    16,
                ),
            ):
                bounded = call(
                    f"/graphs/{graph_name}/query",
                    {
                        "query": query,
                        "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 32},
                    },
                )
                assert bounded["rows"] == [
                    [{"type": "integer", "value": str(expected)}]
                ], bounded
                assert bounded["stats"]["scanned_anchors"] == 32, bounded
                assert bounded["stats"]["memory_bytes"] < 1 << 20, bounded
                print(
                    json.dumps(
                        {
                            "demand_storage": "relational"
                            if relational
                            else "document",
                            "wide_label": graph_name == wide_graph,
                            "query": query,
                            "stats": bounded["stats"],
                        },
                        sort_keys=True,
                    )
                )
        # A value observer upgrades nullness to actual values; whole nodes and
        # labels() retain their complete entity/membership contracts.
        mixed = call(
            f"/graphs/{wide_graph}/query",
            {
                "query": "MATCH(n:D) WHERE antfly.documentKey(n)='n1' RETURN n.body IS NOT NULL,n.body,labels(n)",
                "limits": {"memory_bytes": 16 << 20},
            },
        )
        assert mixed["rows"][0][0] == {"type": "boolean", "value": True}, mixed
        assert mixed["rows"][0][1] == {
            "type": "string",
            "value": 'escaped\\"\n' + "x" * (512 << 10),
        }, mixed
        assert mixed["rows"][0][2] == {
            "type": "list",
            "value": [{"type": "string", "value": "D"}],
        }, mixed
        # Repeated parameter/literal probes share one lazy invariant domain.
        for size in (512, 2048, 4096):
            ids = [f"n{i}" for i in range(size)]
            for literal in (False, True) if size == 512 else (False,):
                domain = (
                    "[" + ",".join("'" + key + "'" for key in ids) + "]"
                    if literal
                    else "$ids"
                )
                bounded = call(
                    f"/graphs/{prefix}/query",
                    {
                        "query": f"MATCH(n:D) WHERE n.id IN {domain} RETURN count(*)",
                        "parameters": {} if literal else {"ids": ids},
                        "limits": {
                            "memory_bytes": 64 << 20,
                            "scanned_anchors": 32,
                            "intermediate_states": 200000,
                        },
                    },
                )
                assert bounded["rows"] == [[{"type": "integer", "value": "32"}]], (
                    bounded
                )
                assert bounded["stats"]["scanned_anchors"] == 32, bounded
                assert bounded["stats"]["intermediate_states"] < 200000, bounded
                print(
                    json.dumps(
                        {
                            "invariant_storage": "relational"
                            if relational
                            else "document",
                            "ids": size,
                            "literal": literal,
                            "stats": bounded["stats"],
                        },
                        sort_keys=True,
                    )
                )
        for label, limits, code in (
            ("D", {"scanned_anchors": 31}, "CypherAnchorBudgetExceeded"),
            ("A", {"scanned_anchors": 31}, "CypherAnchorBudgetExceeded"),
            ("D", {"intermediate_states": 1}, "CypherStateBudgetExceeded"),
        ):
            rejected = admin.post(
                base + f"/graphs/{prefix}/query",
                json={
                    "query": f"MATCH (n:{label}) RETURN count(n.body)",
                    "limits": {"memory_bytes": 8 << 20, **limits},
                },
                timeout=45,
            )
            assert rejected.status_code == 429, rejected.text
            assert rejected.json().get("code") == code, rejected.text
        call(
            f"/auth/v1/users/{reader.auth[0]}",
            {
                "password": reader.auth[1],
                "initial_policies": [
                    {"resource_type": "graph", "resource": prefix, "type": "read"},
                    {"resource_type": "table", "resource": prefix, "type": "read"},
                ],
            },
        )
        call(
            f"/auth/v1/users/{reader.auth[0]}/row-filters/{prefix}",
            {"term": {"tenant": "acme"}},
            method="PUT",
        )
        count("D", reader, (16, 8))
        for session, expected in ((admin, 32), (reader, 16)):
            ordinary = call(
                f"/graphs/{prefix}/query",
                {
                    "query": "MATCH(n:D) RETURN sum(1)",
                    "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 32},
                },
                session=session,
            )
            assert ordinary["rows"] == [
                [{"type": "integer", "value": str(expected)}]
            ], ordinary
            assert ordinary["stats"]["memory_bytes"] < 1 << 20, ordinary
            assert ordinary["stats"]["scanned_anchors"] == 32, ordinary
            print(
                json.dumps(
                    {
                        "ordinary_storage": "relational" if relational else "document",
                        "filtered": session is reader,
                        "stats": ordinary["stats"],
                    },
                    sort_keys=True,
                )
            )
        for session, expected in ((admin, 16), (reader, 8)):
            observed = call(
                f"/graphs/{prefix}/query",
                {
                    "query": "MATCH(n:D) RETURN sum(CASE WHEN n.body IS NOT NULL THEN 1 ELSE 0 END)",
                    "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 32},
                },
                session=session,
            )
            assert observed["rows"] == [
                [{"type": "integer", "value": str(expected)}]
            ], observed
            assert observed["stats"]["scanned_anchors"] == 32, observed
            assert observed["stats"]["memory_bytes"] < 1 << 20, observed
        # The retained scan applies effective row filters once. Rejected rows
        # still consume physical progress before policy evaluation.
        denied_budget = reader.post(
            base + f"/graphs/{prefix}/query",
            json={
                "query": "MATCH (n:D) RETURN count(n.body)",
                "limits": {"memory_bytes": 8 << 20, "scanned_anchors": 31},
            },
            timeout=45,
        )
        assert denied_budget.status_code == 429, denied_budget.text
        assert denied_budget.json()["code"] == "CypherAnchorBudgetExceeded"
        # Predicate values remain real; aggregate-only properties stay borrowed.
        for session, bodies in ((admin, 16), (reader, 8)):
            for pattern, predicate, expected in (
                ("n:D", "", bodies),
                ("n:D", "WHERE true", bodies),
                ("n:D", "WHERE false", 0),
                ("n:D", "WHERE null", 0),
                ("n:D", "WHERE n.tenant='acme'", 8),
                ("n:D {tenant:'acme'}", "", 8),
                ("n:D", "WHERE n.missing IS NULL", bodies),
                ("n:D", "WHERE n.body IS NOT NULL", bodies),
                ("n:D", "WHERE n.body IS NULL", 0),
                ("n", "", bodies),
                ("n:D:A", "", 8),
                ("n:A:A", "", 8),
            ):
                for memory in (1 << 20, 4 << 20) if session is admin else (8 << 20,):
                    bounded = call(
                        f"/graphs/{prefix}/query",
                        {
                            "query": f"MATCH ({pattern}) {predicate} RETURN count(n.body)",
                            "limits": {"memory_bytes": memory},
                        },
                        session=session,
                    )
                    assert bounded["rows"] == [
                        [{"type": "integer", "value": str(expected)}]
                    ], bounded
                    assert bounded["stats"]["memory_bytes"] < memory, bounded
                    assert bounded["stats"]["scanned_anchors"] == 32, bounded
                    if (
                        session is admin
                        and memory == 1 << 20
                        and (pattern != "n:D" or "n.body IS" in predicate)
                    ):
                        print(
                            json.dumps(
                                {
                                    "storage": "relational"
                                    if relational
                                    else "document",
                                    "pattern": pattern,
                                    "predicate": predicate,
                                    "stats": bounded["stats"],
                                },
                                sort_keys=True,
                            )
                        )
            # A predicate observing the count input must see its actual value,
            # rather than the nullness sentinel used by other count statements.
            observed = call(
                f"/graphs/{prefix}/query",
                {
                    "query": "MATCH (n:D) WHERE n.enabled=false RETURN count(n.enabled)",
                    "limits": {"memory_bytes": 1 << 20},
                },
                session=session,
            )
            assert observed["rows"] == [[{"type": "integer", "value": str(bodies)}]], (
                observed
            )
        residual = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH (n:D) WHERE n.id='n1' RETURN count(n.body)",
                "limits": {
                    "memory_bytes": 1 << 20,
                    "scanned_anchors": 1 if relational else 32,
                },
            },
        )
        assert residual["rows"] == [[{"type": "integer", "value": "1"}]], residual
        # Sparse index candidates retain the bounded projection and exact
        # membership semantics, including duplicate and NULL operands.
        for predicate, expected, anchors in (
            ("n.id IN ['n1']", 1, 1),
            ("n.id IN ['n1','n3']", 2, 2),
            ("n.id='n1' OR n.id='n3'", 2, 2),
            ("n.id IN ['n1','n1',null]", 1, 1),
            ("n.id IN []", 0, 0),
            ("n.id IN [null]", 0, 0),
        ):
            for session in (admin, reader):
                visible = expected if session is admin else min(expected, 1)
                memory = 1 << 20
                result = call(
                    f"/graphs/{prefix}/query",
                    {
                        "query": f"MATCH (n:D) WHERE {predicate} RETURN count(n.body)",
                        "limits": {
                            "memory_bytes": memory,
                            "scanned_anchors": max(1, anchors) if relational else 32,
                        },
                    },
                    session=session,
                )
                assert result["rows"] == [
                    [{"type": "integer", "value": str(visible)}]
                ], result
                assert result["stats"]["memory_bytes"] < memory, result
                if relational:
                    assert result["stats"]["scanned_anchors"] <= anchors, result
                if session is admin:
                    print(
                        json.dumps(
                            {
                                "storage": "relational" if relational else "document",
                                "predicate": predicate,
                                "stats": result["stats"],
                            },
                            sort_keys=True,
                        )
                    )
        null_rows = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH(n:D) WHERE n.body IS NULL RETURN count(*)",
                "limits": {"memory_bytes": 1 << 20},
            },
        )
        assert null_rows["rows"] == [[{"type": "integer", "value": "16"}]], null_rows
        if relational:
            exceeded = admin.post(
                base + f"/graphs/{prefix}/query",
                json={
                    "query": "MATCH(n:D) WHERE n.id IN ['n1','n3'] RETURN count(n.body)",
                    "limits": {"memory_bytes": 1 << 20, "scanned_anchors": 1},
                },
                timeout=45,
            )
            assert exceeded.status_code == 429, exceeded.text
            assert exceeded.json()["code"] == "CypherAnchorBudgetExceeded", (
                exceeded.text
            )
        # An effective filter that admits no rows must not evaluate a hidden
        # row's scalar predicate. Progress still accounts for all candidates.
        call(
            f"/auth/v1/users/{reader.auth[0]}/row-filters/{prefix}",
            {"term": {"tenant": "absent"}},
            method="PUT",
        )
        hidden = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH (n:D) WHERE 1/0=0 RETURN count(n.body)",
                "limits": {"memory_bytes": 1 << 20},
            },
            session=reader,
        )
        assert hidden["rows"] == [[{"type": "integer", "value": "0"}]], hidden
        assert hidden["stats"]["scanned_anchors"] == 32, hidden
        sparse_hidden = call(
            f"/graphs/{prefix}/query",
            {
                "query": "MATCH(n:D) WHERE n.id IN ['n1','n3'] AND 1/0=0 RETURN count(n.body)",
                "limits": {"memory_bytes": 1 << 20},
            },
            session=reader,
        )
        assert sparse_hidden["rows"] == [[{"type": "integer", "value": "0"}]], (
            sparse_hidden
        )
        assert sparse_hidden["stats"]["scanned_anchors"] == (2 if relational else 32), (
            sparse_hidden
        )
    finally:
        reader.close()
        admin.close()
        server.stop()


def test_authenticated_cypher_relationship_demand_skips_unused_metadata():
    """Relationship projections share scalar demand without truncating returned entities."""
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    session = requests.Session()
    session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    name = "cypher_edge_demand_" + uuid.uuid4().hex
    base = server.api_url.rstrip("/")
    body = "x" * (128 << 10)

    def call(path, payload=None, method="POST"):
        response = session.request(method, base + path, json=payload, timeout=45)
        assert response.status_code in (200, 201, 202), (
            response.status_code,
            response.text,
            server.log_path.read_text()[-4000:],
        )
        return response.json()

    try:
        call(
            f"/tables/{name}",
            {
                "num_shards": 1,
                "indexes": {
                    "links": {"type": "graph", "edge_types": [{"name": "link"}]}
                },
            },
        )
        deadline = time.monotonic() + 60
        while True:
            index = call(f"/tables/{name}/indexes/links", method="GET")
            ready = index.get("status", {}).get("milestones", {}).get("complete", {})
            if ready.get("reached") and not ready.get("blockers"):
                break
            assert time.monotonic() < deadline, index
            time.sleep(0.1)
        call(
            f"/graphs/{name}",
            {
                "labels": [{"name": "D", "table": name}],
                "relationships": [
                    {
                        "name": "R",
                        "index": "links",
                        "source_label": "D",
                        "target_label": "D",
                        "stored_type": "link",
                    }
                ],
            },
        )
        call(
            f"/tables/{name}/batch",
            {
                "inserts": {"a": {"id": "a"}, "b": {"id": "b"}},
                "graph_writes": [
                    {
                        "index_name": "links",
                        "source": "a",
                        "target": "b",
                        "edge_type": "link",
                        "edge_id": str(i),
                        "metadata_json": json.dumps(
                            {
                                "rank": i,
                                "body": body,
                                "payload": {"rank": i, "body": body, "nil": None},
                            }
                        ),
                    }
                    for i in range(16)
                ],
                "sync_level": "full_index",
            },
        )
        prefix = "MATCH(a:D)-[r:R]->(b:D) WHERE antfly.documentKey(a)='a' RETURN "
        for typed in (False, True):
            if typed:
                call(
                    f"/graphs/{name}/query",
                    {
                        "query": "MATCH(a:D {id:'a'}),(b:D {id:'b'}) CREATE(a)-[r:R {rank:99,body:$body,payload:{rank:99,body:$body}}]->(b) RETURN 1",
                        "parameters": {"body": body},
                        "write_request_id": uuid.uuid4().hex,
                    },
                )
            expected_count, expected_sum = (17, 219) if typed else (16, 120)
            for projection, expected in (
                ("count(*)", expected_count),
                ("count(r)", expected_count),
                ("count(r.body)", expected_count),
                ("sum(r.rank)", expected_sum),
                ("sum(r.payload.rank)", expected_sum),
                ("count(r.payload.body)", expected_count),
                (
                    "sum(CASE WHEN r.payload.body IS NOT NULL THEN 1 ELSE 0 END)",
                    expected_count,
                ),
                ("sum(CASE WHEN r.body IS NOT NULL THEN 1 ELSE 0 END)", expected_count),
            ):
                result = call(
                    f"/graphs/{name}/query",
                    {
                        "query": prefix + projection,
                        "limits": {
                            "memory_bytes": 1 << 20,
                            "explored_edges": expected_count,
                        },
                    },
                )
                assert result["rows"] == [
                    [{"type": "integer", "value": str(expected)}]
                ], result
                assert result["stats"]["explored_edges"] == expected_count, result
                assert result["stats"]["memory_bytes"] < 1 << 20, result
                print(
                    json.dumps(
                        {
                            "edge_demand": projection,
                            "typed": typed,
                            "stats": result["stats"],
                        },
                        sort_keys=True,
                    )
                )
            complete = call(
                f"/graphs/{name}/query",
                {"query": prefix + "r LIMIT 1", "limits": {"memory_bytes": 32 << 20}},
            )
            properties = complete["rows"][0][0]["value"]["properties"]["value"]
            assert properties["body"] == {"type": "string", "value": body}, complete
            assert properties["payload"]["value"]["body"] == {
                "type": "string",
                "value": body,
            }, complete
        denied = session.post(
            base + f"/graphs/{name}/query",
            json={"query": prefix + "count(*)", "limits": {"explored_edges": 16}},
            timeout=45,
        )
        assert (
            denied.status_code == 429
            and denied.json().get("code") == "CypherEdgeBudgetExceeded"
        ), denied.text
    finally:
        session.close()
        server.stop()


@pytest.mark.parametrize("relational", [False, True], ids=["document", "relational"])
def test_authenticated_cypher_nested_and_table_demand(relational, monkeypatch):
    """Nested cells and full entities from another table must not widen reads."""
    write_config = conftest._write_remote_content_e2e_config

    def with_graph_work_budget(*args, **kwargs):
        path = write_config(*args, **kwargs)
        config = json.loads(path.read_text())
        config.setdefault("resources", {}).setdefault("query", {}).setdefault(
            "request_limits", {}
        )["graph"] = {"max_intermediate_states": 1000000}
        path.write_text(json.dumps(config))
        return path

    monkeypatch.setattr(
        conftest, "_write_remote_content_e2e_config", with_graph_work_budget
    )
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip("native Antfly binary is unavailable")
    server = StandaloneAntflyServer(binary, "127.0.0.1", find_free_port(), pgwire=True)
    admin = requests.Session()
    admin.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
    reader = requests.Session()
    name = "cypher_paths_" + uuid.uuid4().hex
    categories = name + "_categories"
    reader.auth = (name + "_reader", uuid.uuid4().hex)
    base = server.api_url.rstrip("/")
    wide = "x" * (2 << 20)

    def call(path, body, *, session=admin, method="POST"):
        response = session.request(
            method,
            (server.url if path.startswith("/auth/") else base) + path,
            json=body,
            timeout=45,
        )
        assert response.status_code in (200, 201, 202), (
            response.status_code,
            response.text,
            server.log_path.read_text()[-5000:],
        )
        return response.json()

    def query(statement, memory=1 << 20, session=admin, work=1000):
        result = call(
            f"/graphs/{name}/query",
            {
                "query": statement,
                "limits": {"memory_bytes": memory, "intermediate_states": work},
            },
            session=session,
        )
        assert result["stats"]["memory_bytes"] < memory, result
        return result

    def integer(value):
        return {"type": "integer", "value": str(value)}

    try:
        if relational:
            call(
                "/sql",
                {
                    "statement": f"CREATE TABLE {name} (id TEXT, tenant TEXT, score BIGINT, body TEXT, payload JSONB)"
                },
            )
        else:
            call(f"/tables/{name}", {"num_shards": 1})
        call(f"/tables/{categories}", {"num_shards": 1})
        documents = {
            "d1": {
                "id": "d1",
                "tenant": "acme",
                "score": 7,
                "body": wide,
                "payload": {"rank": 7, "body": wide, "nil": None},
            },
            "d2": {"id": "d2", "tenant": "other", "score": 0, "payload": None},
            "d3": {"id": "d3", "tenant": "acme", "score": 0},
            "d6": {
                "id": "d6",
                "tenant": "other",
                "score": 99,
                "payload": {"rank": 99, "body": "hidden"},
            },
        }
        call(
            f"/tables/{name}/batch", {"inserts": documents, "sync_level": "full_index"}
        )
        call(
            f"/tables/{categories}/batch",
            {"inserts": {"c1": {"id": "c1"}}, "sync_level": "full_index"},
        )
        call(
            f"/graphs/{name}",
            {
                "labels": [
                    {"name": "D", "table": name},
                    {"name": "Wide", "table": name, "where": {"body": "unmatched"}},
                    {"name": "C", "table": categories},
                ],
                "relationships": [],
            },
        )
        call(
            f"/auth/v1/users/{reader.auth[0]}",
            {
                "password": reader.auth[1],
                "initial_policies": [
                    {"resource_type": "graph", "resource": name, "type": "read"},
                    {"resource_type": "table", "resource": name, "type": "read"},
                    {"resource_type": "table", "resource": categories, "type": "read"},
                ],
            },
        )
        call(
            f"/auth/v1/users/{reader.auth[0]}/row-filters/{name}",
            {"term": {"tenant": "acme"}},
            method="PUT",
        )
        for session, expected_sum, expected_present in (
            (admin, 106, 2),
            (reader, 7, 1),
        ):
            for statement, expected in (
                ("MATCH(d:D) RETURN sum(d.payload.rank)", expected_sum),
                ("MATCH(d:D) OPTIONAL MATCH(d:C) RETURN sum(d.score)", expected_sum),
                ("MATCH(d:D) RETURN count(d.payload.body)", expected_present),
                (
                    "MATCH(d:D) RETURN sum(CASE WHEN d.payload.body IS NOT NULL THEN 1 ELSE 0 END)",
                    expected_present,
                ),
                ("MATCH(d:D) WITH d AS x RETURN sum(x.payload.rank)", expected_sum),
                ("MATCH(d:D) RETURN sum(d['payload']['rank'])", expected_sum),
                (
                    "CALL { MATCH(d:D) RETURN d } RETURN sum(d.payload.rank)",
                    expected_sum,
                ),
                (
                    "CALL { MATCH(d:D) RETURN d.payload AS p } RETURN sum(p.rank)",
                    expected_sum,
                ),
                (
                    "MATCH(d:D) CALL { WITH d RETURN d.payload AS p } RETURN sum(p.rank)",
                    expected_sum,
                ),
                ("MATCH(d:D) WITH d.payload AS p RETURN sum(p.rank)", expected_sum),
                (
                    "MATCH(d:D) WITH d.payload AS p WITH p AS q RETURN sum(q.rank)",
                    expected_sum,
                ),
            ):
                result = query(statement, session=session)
                assert result["rows"] == [[integer(expected)]], result
                print(
                    json.dumps(
                        {
                            "path_storage": "relational" if relational else "document",
                            "filtered": session is reader,
                            "query": statement,
                            "stats": result["stats"],
                        },
                        sort_keys=True,
                    )
                )
            for projection in ("d.payload{.rank}", "d['payload']{.rank}"):
                result = query(
                    f"MATCH(d:D) WHERE d.id='d1' RETURN {projection}", session=session
                )
                assert result["rows"] == [
                    [{"type": "map", "value": {"rank": integer(7)}}]
                ], result
            result = query(
                "MATCH(d:D) WHERE d.id='d1' WITH d.payload AS p RETURN p{.rank} AS ranked,p{answer:42} AS computed",
                session=session,
            )
            assert result["rows"] == [
                [
                    {"type": "map", "value": {"rank": integer(7)}},
                    {"type": "map", "value": {"answer": integer(42)}},
                ]
            ], result
            for returned in ("c", "labels(c)"):
                result = query(
                    f"MATCH(d:D),(c:C) RETURN {returned},sum(d.score)", session=session
                )
                assert result["rows"][0][1] == integer(expected_sum), result
                if returned == "c":
                    assert (
                        result["rows"][0][0]["value"]["properties"]["value"]["id"][
                            "value"
                        ]
                        == "c1"
                    ), result
                else:
                    assert result["rows"][0][0] == {
                        "type": "list",
                        "value": [{"type": "string", "value": "C"}],
                    }, result
                print(
                    json.dumps(
                        {
                            "table_storage": "relational" if relational else "document",
                            "filtered": session is reader,
                            "returned": returned,
                            "stats": result["stats"],
                        },
                        sort_keys=True,
                    )
                )
        result = query(
            "MATCH(d:D) RETURN d.id,d.payload.rank,d.payload.body IS NOT NULL,d.payload IS NULL ORDER BY d.id"
        )
        expected = [
            ("d1", 7, True, False),
            ("d2", None, False, True),
            ("d3", None, False, True),
            ("d6", 99, True, False),
        ]
        assert result["rows"] == [
            [
                {"type": "string", "value": key},
                integer(rank) if rank is not None else {"type": "null", "value": None},
                {"type": "boolean", "value": present},
                {"type": "boolean", "value": nil},
            ]
            for key, rank, present, nil in expected
        ], result
        # A complete ancestor observer must override its partial descendants.
        result = query(
            "MATCH(d:D) WHERE antfly.documentKey(d)='d1' RETURN d.payload,d.payload.rank",
            64 << 20,
            work=1000000,
        )
        assert result["rows"][0][0]["value"]["body"] == {
            "type": "string",
            "value": wide,
        }, result
        assert result["rows"][0][1] == integer(7), result
        # Aliases on the same physical table still share a complete identity view.
        result = query(
            "MATCH(d:D),(x:D) WHERE antfly.documentKey(d)='d6' AND antfly.documentKey(x)='d6' RETURN x,d.payload.rank",
        )
        properties = result["rows"][0][0]["value"]["properties"]["value"]
        assert properties["id"]["value"] == "d6", result
        assert properties["tenant"]["value"] == "other", result
        assert properties["score"] == integer(99), result
        assert properties["payload"]["value"]["body"]["value"] == "hidden", result
        assert result["rows"][0][1] == integer(99), result
        # Non-map ancestors preserve Cypher's property-access type error, while
        # presence and complete-value observers retain their ordinary behavior.
        call(
            f"/tables/{name}/batch",
            {
                "inserts": {
                    "d4": {"id": "d4", "tenant": "other", "score": 0, "payload": 42},
                    "d5": {"id": "d5", "tenant": "other", "score": 0, "payload": [{}]},
                },
                "sync_level": "full_index",
            },
        )
        for key in ("d4", "d5"):
            for projection in (
                "RETURN d.payload.rank",
                "WITH d.payload AS p RETURN p.rank",
                "RETURN d['payload']['rank']",
                "RETURN d.payload{.rank}",
                "WITH d.payload AS p RETURN p{}",
            ):
                invalid = admin.post(
                    base + f"/graphs/{name}/query",
                    json={
                        "query": f"MATCH(d:D) WHERE antfly.documentKey(d)='{key}' {projection}",
                        "limits": {"memory_bytes": 1 << 20},
                    },
                    timeout=45,
                )
                assert (
                    invalid.status_code == 400
                    and invalid.json().get("code") == "CypherTypeMismatch"
                ), invalid.text
        result = query(
            "MATCH(d:D) WHERE antfly.documentKey(d) IN ['d4','d5'] RETURN d.id,d.payload,d.payload IS NULL ORDER BY d.id"
        )
        assert result["rows"] == [
            [
                {"type": "string", "value": "d4"},
                integer(42),
                {"type": "boolean", "value": False},
            ],
            [
                {"type": "string", "value": "d5"},
                {"type": "list", "value": [{"type": "map", "value": {}}]},
                {"type": "boolean", "value": False},
            ],
        ], result
    finally:
        reader.close()
        admin.close()
        server.stop()


def test_authenticated_cypher_lineage_scopes_and_endpoint_domains(monkeypatch):
    """WITH and local names preserve narrow reads; mappings constrain endpoints."""
    write_config = conftest._write_remote_content_e2e_config

    def with_graph_work_budget(*args, **kwargs):
        path = write_config(*args, **kwargs)
        config = json.loads(path.read_text())
        config.setdefault("resources", {}).setdefault("query", {}).setdefault(
            "request_limits", {}
        )["graph"] = {"max_intermediate_states": 1000000}
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
    base = server.api_url.rstrip("/")
    name = "cypher_lineage_" + uuid.uuid4().hex

    def call(path, body=None, method="POST"):
        response = session.request(method, base + path, json=body, timeout=45)
        assert response.status_code in (200, 201, 202), (
            response.status_code,
            response.text,
        )
        return response.json()

    def query(statement, memory=1 << 20):
        result = call(
            f"/graphs/{name}/query",
            {
                "query": statement,
                "limits": {
                    "memory_bytes": memory,
                    "intermediate_states": 1000000,
                },
            },
        )
        assert result["stats"]["memory_bytes"] < memory, result
        print(json.dumps({"lineage_query": statement, "stats": result["stats"]}))
        return result

    def integer(n):
        return {"type": "integer", "value": str(n)}

    try:
        call(
            f"/tables/{name}",
            {
                "num_shards": 1,
                "indexes": {
                    "links": {"type": "graph", "edge_types": [{"name": "link"}]},
                },
            },
        )
        call(f"/tables/{name}_c", {"num_shards": 1})
        deadline = time.monotonic() + 60
        while True:
            index = call(f"/tables/{name}/indexes/links", method="GET")
            ready = index.get("status", {}).get("milestones", {}).get("complete", {})
            if ready.get("reached") and not ready.get("blockers"):
                break
            assert time.monotonic() < deadline, index
            time.sleep(0.1)
        call(
            f"/tables/{name}/batch",
            {"inserts": {"d1": {"score": 7}}, "sync_level": "full_index"},
        )
        call(
            f"/tables/{name}_c/batch",
            {"inserts": {"c1": {"id": "c1"}}, "sync_level": "full_index"},
        )
        call(
            f"/graphs/{name}",
            {
                "labels": [
                    {"name": "D", "table": name},
                    {"name": "C", "table": name + "_c"},
                ],
                "relationships": [
                    {
                        "name": "R",
                        "stored_type": "link",
                        "index": "links",
                        "source_label": "D",
                        "target_label": "C",
                    }
                ],
            },
        )
        call(
            f"/graphs/{name}/query",
            {
                "query": "MATCH(d:D),(c:C) CREATE(d)-[:R]->(c) RETURN 1",
                "write_request_id": uuid.uuid4().hex,
            },
        )
        wide = "x" * (2 << 20)
        call(
            f"/tables/{name}/batch",
            {
                "inserts": {
                    "d1": {
                        "score": 7,
                        "body": wide,
                        "payload": {"rank": 7, "body": wide},
                    }
                },
                "sync_level": "full_index",
            },
        )
        for statement in (
            "MATCH(d:D) WITH d.payload AS p RETURN sum(p.rank)",
            "MATCH(d:D) WITH d.payload AS p WITH p AS q RETURN sum(q.rank)",
            "MATCH(d:D) RETURN sum(d['payload']['rank'])",
            "CALL { MATCH(d:D) RETURN d } RETURN sum(d.payload.rank)",
            "CALL { MATCH(d:D) RETURN d.payload AS p } RETURN sum(p.rank)",
            "MATCH(d:D) CALL { WITH d RETURN d.payload AS p } RETURN sum(p.rank)",
            "CALL { CALL { MATCH(d:D) RETURN d } RETURN d AS x } RETURN sum(x.payload.rank)",
            "CALL { MATCH(d:D) RETURN d.payload } RETURN sum(`d.payload`.rank)",
        ):
            assert query(statement)["rows"] == [[integer(7)]]
        for statement in (
            "MATCH(d:D) RETURN d.payload{.rank}",
            "MATCH(d:D) RETURN d['payload']{.rank}",
            "MATCH(d:D) WITH d.payload AS p RETURN p{.rank}",
        ):
            assert query(statement)["rows"] == [
                [{"type": "map", "value": {"rank": integer(7)}}]
            ]
        for statement in (
            "MATCH(d:D)-[:R]->(c) RETURN c,sum(d.score)",
            "MATCH(c)<-[:R]-(d:D) RETURN c,sum(d.score)",
        ):
            result = query(statement)
            assert len(result["rows"]) == 1, result
            assert result["rows"][0][1] == integer(7), result
            assert (
                result["rows"][0][0]["value"]["properties"]["value"]["id"]["value"]
                == "c1"
            ), result
        for local in ("x", "d"):
            assert query(f"MATCH(d:D) RETURN [{local} IN [1,2] | {local}]")["rows"] == [
                [
                    {
                        "type": "list",
                        "value": [integer(1), integer(2)],
                    }
                ]
            ]
        # A full ancestor still upgrades every partial alias observer.
        result = query("MATCH(d:D) WITH d.payload AS p RETURN p,p.rank", 64 << 20)
        assert result["rows"][0][0]["value"]["body"]["value"] == wide, result
        assert result["rows"][0][1] == integer(7), result
        # Shape-only projections preserve maps, NULL, missing values and errors.
        call(
            f"/tables/{name}/batch",
            {
                "inserts": {
                    "d1": {
                        "rank": 9223372036854775808,
                        "payload": {"rank": 7, "body": 9223372036854775808},
                    },
                },
                "sync_level": "full_index",
            },
        )
        assert query(
            "MATCH(d:D) WITH d{answer:42} WHERE d.rank IS NULL RETURN d.answer"
        )["rows"] == [[integer(42)]]
        for statement in (
            "MATCH(d:D) RETURN sum(d['payload']['rank'])",
            "CALL { MATCH(d:D) RETURN d } RETURN sum(d.payload.rank)",
            "CALL { MATCH(d:D) RETURN d.payload AS p } RETURN sum(p.rank)",
        ):
            assert query(statement)["rows"] == [[integer(7)]]
        for projection in ("d.payload{.rank}", "d['payload']{.rank}"):
            assert query(f"MATCH(d:D) RETURN {projection}")["rows"] == [
                [
                    {
                        "type": "map",
                        "value": {"rank": integer(7)},
                    }
                ]
            ]
        assert query("MATCH(d:D) RETURN d.payload{answer:42}")["rows"] == [
            [
                {
                    "type": "map",
                    "value": {"answer": integer(42)},
                }
            ]
        ]
        for payload in (None, {}, 42, []):
            call(
                f"/tables/{name}/batch",
                {"inserts": {"d1": {"payload": payload}}, "sync_level": "full_index"},
            )
            for projection in (
                "d.payload{.rank}",
                "d.payload{}",
                "d['payload']{answer:42}",
            ):
                statement = f"MATCH(d:D) RETURN {projection}"
                if payload is None or isinstance(payload, dict):
                    result = query(statement)
                    if payload is None:
                        assert result["rows"] == [[{"type": "null", "value": None}]], (
                            result
                        )
                    else:
                        expected = (
                            {"rank": {"type": "null", "value": None}}
                            if ".rank" in projection
                            else (
                                {"answer": integer(42)}
                                if "answer" in projection
                                else {}
                            )
                        )
                        assert result["rows"] == [
                            [{"type": "map", "value": expected}]
                        ], result
                else:
                    response = session.post(
                        base + f"/graphs/{name}/query",
                        json={"query": statement},
                        timeout=45,
                    )
                    assert (
                        response.status_code == 400
                        and response.json()["code"] == "CypherTypeMismatch"
                    ), response.text
        # Unused document values outside the Cypher integer range must never
        # be evaluated because a lexical local happens to share their name.
        call(
            f"/tables/{name}/batch",
            {
                "inserts": {
                    "d1": {
                        "payload": {"rank": 9223372036854775808},
                    }
                },
                "sync_level": "full_index",
            },
        )
        for local in ("x", "d"):
            result = query(
                f"MATCH(d:D) RETURN [{local} IN [{{payload:{{rank:7}}}}] | {local}.payload.rank]"
            )
            assert result["rows"] == [[{"type": "list", "value": [integer(7)]}]], result
        for statement in (
            "MATCH(d:D) RETURN d['payload']['rank']",
            "MATCH(d:D) RETURN count(d['payload']['rank'])",
            "MATCH(d:D) RETURN d.payload{.rank}",
            "CALL { MATCH(d:D) RETURN d } RETURN sum(d.payload.rank)",
        ):
            response = session.post(
                base + f"/graphs/{name}/query", json={"query": statement}, timeout=45
            )
            assert (
                response.status_code == 400
                and response.json()["code"] == "CypherNumericOverflow"
            ), response.text
        assert query("MATCH(d:D) RETURN all(d IN [1,2] WHERE d>0)")["rows"] == [
            [
                {
                    "type": "boolean",
                    "value": True,
                }
            ]
        ]
        for statement in (
            "MATCH(d:D) RETURN reduce(d=0,x IN [1,2] | d+x)",
            "MATCH(d:D) RETURN reduce(x=0,d IN [1,2] | x+d)",
        ):
            assert query(statement)["rows"] == [[integer(3)]]
    finally:
        session.close()
        server.stop()
