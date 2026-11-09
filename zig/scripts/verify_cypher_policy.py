#!/usr/bin/env python3
"""Qualify native Cypher row authority against a disposable authenticated server.

Requires the server's trusted-principal signer and ANTFLY_CYPHER_TEST_AUTH.
Only uniquely named campaign fixtures are removed.
"""

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
    parser.add_argument(
        "--search", action="store_true", help="Also qualify protected full-text search"
    )
    args = parser.parse_args()
    header = (
        "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode()
    )
    table = "cypher_policy_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = enabled = False

    def call(path, body=None, method="POST", accepted=(200, 201, 202)):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": header},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                status, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, content = error.code, error.read()
        assert status in accepted, (path, status, content.decode())
        return status, json.loads(content) if content else None

    def sql(statement):
        return call("/sql", {"statement": statement})[1]

    def query(statement, accepted=(200,)):
        return call(f"/graphs/{graph}/query", {"query": statement}, accepted=accepted)

    def await_rows(expected=None):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status, result = query(
                "MATCH (n:Document) RETURN n.id ORDER BY n.id", (200, 403, 409, 503)
            )
            if status == 403:
                assert result["error"] == "RowPolicyAuthenticationRequired", result
            if status == 200 and (
                result["rows"] == expected
                if expected is not None
                else [{"type": "string", "value": "b"}] in result["rows"]
            ):
                return
            time.sleep(0.2)
        raise AssertionError(
            f"row-policy publication did not reach the expected serving state: {status} {result}"
        )

    # Readiness probes are read-only; never retry fixture mutations after a
    # transport failure with an unknown outcome.
    readiness_deadline = time.monotonic() + 30
    while True:
        try:
            call("/graphs", method="GET", accepted=(200,))
            break
        except urllib.error.URLError:
            if time.monotonic() >= readiness_deadline:
                raise
            time.sleep(0.2)

    try:
        sql(f"CREATE TABLE {table} (id TEXT, title TEXT, tenant TEXT)")
        created_table = True
        call(f"/tables/{table}/indexes/links", {"type": "graph"})
        if args.search:
            call(f"/tables/{table}/indexes/text", {"type": "full_text"})
        sql(
            f"INSERT INTO {table} (id,title,tenant) VALUES ('a','Alpha','visible'),('b','Beta','hidden')"
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [{"name": "Document", "table": table}],
                "relationships": [
                    {
                        "name": "CITES",
                        "index": "links",
                        "source_label": "Document",
                        "target_label": "Document",
                        "stored_type": "cites",
                    }
                ],
            },
        )
        created_graph = True
        query(
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[:CITES]->(b)"
        )
        sql(
            f"CREATE POLICY visible ON {table} USING (tenant = 'visible') WITH CHECK (tenant = 'visible')"
        )
        sql(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        enabled = True
        await_rows([[{"type": "string", "value": "a"}]])
        _, traversal = query("MATCH (a:Document)-[:CITES]->(b) RETURN a.id,b.id")
        assert traversal["rows"] == [], traversal
        if args.search:
            _, visible_search = query(
                f"CALL antfly.search('{table}', {{query:'title:Alpha',mode:'text',limit:10}}) YIELD node,score RETURN node.id,score"
            )
            assert len(visible_search["rows"]) == 1 and visible_search["rows"][0][
                0
            ] == {"type": "string", "value": "a"}, visible_search
            baseline_score = visible_search["rows"][0][1]
            sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
            enabled = False
            await_rows()
            sql(f"INSERT INTO {table} (id,title,tenant) VALUES ('h','Alpha','hidden')")
            sql(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            enabled = True
            await_rows([[{"type": "string", "value": "a"}]])
            _, rescored = query(
                f"CALL antfly.search('{table}', {{query:'title:Alpha',mode:'text',limit:10}}) YIELD node,score RETURN node.id,score"
            )
            assert rescored["rows"] == [
                [{"type": "string", "value": "a"}, baseline_score]
            ], (visible_search, rescored)
            _, hidden_search = query(
                f"CALL antfly.search('{table}', {{query:'title:Beta',mode:'text',limit:10}}) YIELD node RETURN node.id"
            )
            assert hidden_search["rows"] == [], hidden_search

        _, created = query(
            "CREATE (n:Document {id:'c',title:'Gamma',tenant:'visible'}) RETURN n"
        )
        assert created["complete"] is True and created["commit"]["status"] in (
            "committed",
            "committed_pending",
            "committed_repair_required",
        ), created
        _, denied = query(
            "CREATE (n:Document {id:'d',tenant:'hidden'}) RETURN n", (403,)
        )
        assert denied["error"] == "RowPolicyDenied", denied
        await_rows(
            [[{"type": "string", "value": "a"}], [{"type": "string", "value": "c"}]]
        )
        composed = sql(
            f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (n.id AS id)) g ORDER BY g.id"
        )
        assert composed["rows"] == [["a"], ["c"]], composed
        print(
            "Native authenticated Cypher row-policy campaign: scans, traversal, writes, rejection, GRAPH_TABLE passed"
            + (
                "; protected full-text hits and score invariance passed"
                if args.search
                else ""
            )
        )
    finally:
        if enabled:
            sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
            await_rows()
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE", accepted=(200, 202, 204))
        if created_table:
            sql(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
