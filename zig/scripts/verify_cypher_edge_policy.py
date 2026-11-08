#!/usr/bin/env python3
"""Qualify edge-only native row authorization without rewriting SQL documents."""

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
    table = "cypher_edge_policy_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = enabled = False

    def call(path, body=None, method="POST", accepted=(200, 201, 202, 204)):
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
                status, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, content = error.code, error.read()
        assert status in accepted, (path, status, content.decode())
        return json.loads(content) if content else None

    def sql(statement):
        deadline = time.monotonic() + 90
        while True:
            result = call(
                "/sql", {"statement": statement}, accepted=(200, 201, 202, 204, 409)
            )
            if result.get("code") != "40001":
                assert "code" not in result, result
                return result
            # The SQL contract explicitly reports a noncommitted statement.
            # Fresh replicated owners can need time for route admission.
            assert (
                result.get("retryable") and result.get("transaction_status") == "idle"
            ), result
            assert time.monotonic() < deadline, result
            time.sleep(0.2)

    def query(statement, accepted=(200, 202)):
        return call(f"/graphs/{graph}/query", {"query": statement}, accepted=accepted)

    def write(statement):
        result = query(statement)
        receipt = result["commit"]
        deadline = time.monotonic() + 90
        while receipt["status"] != "committed":
            status = call(
                f"/transactions/{receipt['transaction_id']}/status", method="GET"
            )
            assert status["disposition"] != "aborted", status
            if status["disposition"] == "committed" and status["durable"]:
                break
            assert time.monotonic() < deadline, status
            time.sleep(0.2)
        return result

    try:
        sql(f"CREATE TABLE {table} (id TEXT, payload JSONB)")
        created_table = True
        call(f"/tables/{table}/indexes/links", {"type": "graph"})
        sql(
            f"INSERT INTO {table} (id,payload) VALUES ('a','null'::jsonb),('b','{{}}'::jsonb),('denied',NULL),('hidden','{{}}'::jsonb)"
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [{"name": "D", "table": table}],
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
        )
        created_graph = True
        write("MATCH (a:D {id:'denied'}),(b:D {id:'b'}) CREATE (a)-[:LINK]->(b)")
        sql(
            f"CREATE POLICY edge_source ON {table} USING (id <> 'hidden') WITH CHECK (payload IS NOT NULL)"
        )
        sql(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        enabled = True
        deadline = time.monotonic() + 90
        while True:
            result = query(
                "MATCH (n:D) RETURN n.id ORDER BY n.id", accepted=(200, 403, 409, 503)
            )
            if [row[0]["value"] for row in result.get("rows", [])] == [
                "a",
                "b",
                "denied",
            ]:
                break
            assert time.monotonic() < deadline, result
            time.sleep(0.2)
        for statement in (
            "CALL { MATCH (a:D {id:'a'}),(b:D {id:'b'}) CREATE (a)-[:LINK {age:1}]->(b) }",
            "CALL { CALL { MATCH (:D {id:'a'})-[r:LINK]->() SET r.age=2 } }",
            "CALL { MATCH (:D {id:'a'})-[r:LINK]->() REMOVE r.age }",
            "CALL { MATCH (:D {id:'a'})-[r:LINK]->() DELETE r }",
        ):
            result = write(statement)
            assert result["rows"] == [] and not result["read_only"], result
        for statement in (
            "MATCH (a:D {id:'denied'}),(b:D {id:'b'}) CREATE (a)-[:LINK]->(b)",
            "MATCH (:D {id:'denied'})-[r:LINK]->() SET r.age=2",
            "MATCH (:D {id:'denied'})-[r:LINK]->() DELETE r",
        ):
            failure = query(statement, accepted=(403,))
            assert failure["code"] == "RowPolicyDenied", failure
            assert failure["write_outcome"] == "aborted", failure
        assert (
            query("MATCH ()-[r:LINK]->() RETURN count(r)")["rows"][0][0]["value"] == "1"
        )
        # The metadata SQL endpoint fails closed on protected relational
        # scans. Inspect durable SQL nulls after publishing policy disable;
        # all graph mutations above execute with the policy enabled.
        sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        enabled = False
        deadline = time.monotonic() + 90
        while True:
            durable = call(
                "/sql",
                {"statement": f"SELECT id,payload IS NULL FROM {table} ORDER BY id"},
                accepted=(200, 403, 409, 503),
            )
            if durable.get("rows") == [
                ["a", False],
                ["b", False],
                ["denied", True],
                ["hidden", False],
            ]:
                break
            assert time.monotonic() < deadline, durable
            time.sleep(0.2)
        print(
            "8 authenticated edge-only SQL-null, unit-CALL and WITH CHECK checks passed"
        )
    finally:
        if enabled:
            sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            sql(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
