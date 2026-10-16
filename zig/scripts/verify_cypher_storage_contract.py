#!/usr/bin/env python3
"""Qualify lossless graph storage boundaries through authenticated owners."""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    args = parser.parse_args()
    authorization = (
        "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode()
    )
    prefix = "cypher_storage_" + uuid.uuid4().hex
    table, temporal, graph = prefix, prefix + "_time", prefix + "_graph"
    tables = []
    created_graph = False

    def call(
        path, body=None, *, method="POST", accepted=(200, 201, 202, 204), setup=False
    ):
        deadline = time.monotonic() + 60
        while True:
            request = urllib.request.Request(
                args.url.rstrip("/") + "/db/v1" + path,
                data=None if body is None else json.dumps(body).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": authorization,
                },
                method=method,
            )
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    code, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, raw = error.code, error.read()
            # Fixture replay requires an explicit noncommit result. Never
            # replay a graph write or an unknown transaction outcome.
            if setup and code in (409, 503):
                failure = json.loads(raw) if raw.startswith(b"{") else {}
                aborted = (
                    code == 409
                    and raw == b"batch transaction conflicted"
                    or failure.get("code") in ("40001", "53300")
                    and failure.get("transaction_status") == "idle"
                )
                if aborted and time.monotonic() < deadline:
                    time.sleep(0.2)
                    continue
            assert code in accepted, (path, code, raw.decode())
            return json.loads(raw) if raw else None

    def sql(statement):
        return call("/sql", {"statement": statement}, setup=True)

    def query(statement, **options):
        result = call(f"/graphs/{graph}/query", {"query": statement, **options})
        if receipt := result.get("commit"):
            deadline = time.monotonic() + 90
            while receipt["status"] != "committed":
                status = call(
                    f"/transactions/{receipt['transaction_id']}/status",
                    method="GET",
                    accepted=(200, 409, 503),
                )
                assert status.get("disposition") != "aborted", status
                if status.get("disposition") == "committed" and status.get("durable"):
                    break
                assert time.monotonic() < deadline, status
                time.sleep(0.2)
        return result["rows"]

    def native_weight(target):
        result = call(
            f"/tables/{table}/query",
            {
                "graph_queries": {
                    "native": {
                        "index": "links",
                        "shortest_path": {
                            "from": {"key": "a"},
                            "to": {"key": target},
                            "edge_types": ["link"],
                            "max_depth": 1,
                        },
                    }
                },
                "limit": 1,
            },
        )
        paths = result["responses"][0]["graph_results"]["native"]["paths"]
        assert len(paths) == 1, result
        return paths[0]["path"]["weight_sum"]

    try:
        call(
            f"/tables/{table}", {"name": table, "indexes": {"links": {"type": "graph"}}}
        )
        tables.append(table)
        sql(f"CREATE TABLE {temporal}(id TEXT, ts TIMESTAMPTZ)")
        tables.append(temporal)
        edge_id = "cypher:" + uuid.uuid4().hex
        call(
            f"/tables/{table}/batch",
            {
                "inserts": {key: {"id": key} for key in ("a", "b", "c")},
                "graph_writes": [
                    {
                        "index_name": "links",
                        "source": "a",
                        "target": target,
                        "edge_type": "link",
                        "edge_id": edge_id,
                        "weight": 3.5 + tag,
                        "created_at": 123 + tag,
                        "updated_at": 456 + tag,
                        "metadata_json": json.dumps(
                            {"antfly_cypher": "customer-value", "tag": tag}
                        ),
                    }
                    for tag, target in enumerate(("b", "c"))
                ],
                "sync_level": "full_index",
            },
            setup=True,
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [
                    {"name": "D", "table": table},
                    {"name": "T", "table": temporal},
                ],
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
        created_graph = True
        select = "MATCH (a:D)-[r:R]->(b:D) RETURN elementId(r),r.antfly_cypher,r.tag ORDER BY b.id"
        deadline = time.monotonic() + 60
        while True:
            readiness = call(
                f"/graphs/{graph}/query",
                {"query": "MATCH (n) RETURN n LIMIT 0"},
                accepted=(200, 409, 503),
            )
            if readiness.get("complete"):
                break
            assert time.monotonic() < deadline, readiness
            time.sleep(0.2)
        before = query(select)
        assert len(before) == 2 and before[0][0] != before[1][0], before
        assert [row[1]["value"] for row in before] == ["customer-value"] * 2, before
        changed = query(
            "MATCH (a:D)-[r:R]->(b:D) SET r.tag=r.tag+10 RETURN elementId(r),r.antfly_cypher,r.tag ORDER BY b.id"
        )
        assert [row[0] for row in changed] == [row[0] for row in before], changed
        assert [row[2]["value"] for row in changed] == ["10", "11"], changed
        assert query(select) == changed
        assert [native_weight(target) for target in ("b", "c")] == [3.5, 4.5]
        query("MATCH ()-[r:R]->() SET r.weight=r.weight+1")
        assert [native_weight(target) for target in ("b", "c")] == [4.5, 5.5]
        query("MATCH ()-[r:R]->() REMOVE r.weight")
        query("MATCH ()-[r:R]->() SET r.note='kept'")
        assert [native_weight(target) for target in ("b", "c")] == [4.5, 5.5]
        assert sql(
            f"SELECT count(*) FROM GRAPH_TABLE({graph} MATCH ()-[r IS R]->() COLUMNS(r AS edge)) g"
        )["rows"] == [["2"]]
        epoch = 1577934245000000000
        for _ in range(2):
            assert query(
                "MERGE (n:T {id:'a',ts:$epoch}) RETURN n.ts",
                parameters={"epoch": epoch},
            ) == [[{"type": "integer", "value": str(epoch)}]]
        assert sql(f"SELECT count(*) FROM {temporal}")["rows"] == [["1"]]
        for statement in (
            "CREATE (:T {ts:'2020-01-02T03:04:05Z'})",
            "MERGE (:T {ts:'2020-01-02T04:04:05+01:00'})",
            "MATCH (n:T) SET n.ts='2020-01-02T03:04:05Z'",
            "CREATE (:D {_edges:{links:{link:[{target:'b'}]}}})",
            "MATCH (n:D {id:'a'}) SET n += {_embeddings:{dense:[1,0]}}",
        ):
            failure = call(
                f"/graphs/{graph}/query", {"query": statement}, accepted=(400,)
            )
            assert failure["code"] in (
                "CypherTypeMismatch",
                "CypherInvalidPropertyValue",
            ), failure
            assert failure["write_outcome"] == "not_started", failure
        assert query("MATCH (n:T) RETURN n.ts") == [
            [{"type": "integer", "value": str(epoch)}]
        ]
        assert query(select) == changed
        query("MATCH ()-[r:R]->() DELETE r")
        assert query("MATCH ()-[r:R]->() RETURN count(r)") == [
            [{"type": "integer", "value": "0"}]
        ]
        print(
            "Authenticated lossless Cypher storage contract passed: timestamps, ingestion controls, native metadata, tuple identities, SQL, and deletes"
        )
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        for name in reversed(tables):
            call(f"/tables/{name}", method="DELETE")


if __name__ == "__main__":
    main()
