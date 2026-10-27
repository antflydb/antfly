#!/usr/bin/env python3
"""Compare native Cypher mutation postimages with durable SQL rows."""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid

from cypher_qualification_retry import retry_allowed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    args = parser.parse_args()
    authorization = (
        "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode()
    )
    table = "cypher_normalize_" + uuid.uuid4().hex
    graph = table + "_graph"
    filtered = graph + "_filtered"
    created_table = created_graph = created_filtered = False

    def call(path, body=None, method="POST", expected_status=None, read_only=False):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": authorization,
            },
            method=method,
        )
        for attempt in range(40):
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    status, content = response.status, response.read()
            except urllib.error.HTTPError as error:
                status, content = error.code, error.read()
            if (
                attempt < 39
                and expected_status != status
                and retry_allowed(
                    status, content, path=path, body=body, read_only=read_only
                )
            ):
                time.sleep(0.25)
                continue
            break
        assert (
            status in (200, 201, 202, 204)
            if expected_status is None
            else status == expected_status
        ), (path, status, content.decode())
        return json.loads(content) if content else None

    def sql(statement, *, read_only=False):
        return call("/sql", {"statement": statement}, read_only=read_only)

    def query(statement):
        result = call(f"/graphs/{graph}/query", {"query": statement})
        assert result["complete"] is True, result
        return result

    def verify(statement, expected):
        returned = query(statement)
        assert returned["rows"] == [
            [
                (
                    {"type": "null", "value": None}
                    if value is None
                    else {"type": "integer", "value": str(value)}
                )
                for value in expected
            ]
        ], returned
        stored = sql(f"SELECT amount,total FROM {table}", read_only=True)
        assert stored["rows"] == [
            [None if value is None else str(value) for value in expected]
        ], (returned, stored)

    try:
        sql(
            f"CREATE TABLE {table} (amount BIGINT DEFAULT 4, total BIGINT GENERATED ALWAYS AS (amount * 3) STORED)"
        )
        created_table = True
        call(
            f"/graphs/{graph}",
            {"labels": [{"name": "Document", "table": table}], "relationships": []},
        )
        created_graph = True
        verify(
            "CREATE (n:Document {amount:null}) RETURN n.amount,n.total", [None, None]
        )
        query("MATCH (n:Document) DELETE n")
        assert sql(f"SELECT amount,total FROM {table}", read_only=True)["rows"] == []
        # Creating and deleting a new identity must not stage a durable delete.
        query("CREATE (n:Document) DELETE n")
        assert sql(f"SELECT amount,total FROM {table}", read_only=True)["rows"] == []
        call(
            f"/graphs/{filtered}",
            {"labels": [{"name": "Document", "table": table, "where": {"total": 12}}]},
        )
        created_filtered = True
        for statement in (
            "CREATE (n:Document) RETURN n.amount,n.total",
            "MERGE (n:Document) RETURN n.amount,n.total",
        ):
            result = call(f"/graphs/{filtered}/query", {"query": statement})
            assert [cell["value"] for cell in result["rows"][0]] == ["4", "12"], result
            assert sql(f"SELECT amount,total FROM {table}", read_only=True)["rows"] == [
                ["4", "12"]
            ]
            query("MATCH (n:Document) DELETE n")
        for value in ("null", "7"):
            failure = call(
                f"/graphs/{filtered}/query",
                {"query": f"CREATE (n:Document {{amount:{value}}}) RETURN n"},
                expected_status=400,
            )
            assert failure["code"] == "CypherLabelPredicateViolation", failure
            assert (
                sql(f"SELECT amount,total FROM {table}", read_only=True)["rows"] == []
            )
        verify("CREATE (n:Document) RETURN n.amount,n.total", [4, 12])
        verify(
            "MATCH (n:Document) WITH n AS a,n AS b SET a.amount=7 RETURN b.amount,b.total",
            [7, 21],
        )
        verify(
            "MATCH (n:Document) REMOVE n.amount RETURN n.amount,n.total", [None, None]
        )
        properties = query("MATCH (n:Document) RETURN keys(n),properties(n)")["rows"][0]
        assert properties[0]["type"] == "list" and properties[1]["type"] == "map"
        names = {value["value"] for value in properties[0]["value"]}
        assert not names.intersection({"amount", "total"}), properties
        assert not {"amount", "total"}.intersection(properties[1]["value"]), properties
        print(
            "Native Cypher normalization checks passed: defaults, explicit nulls, CREATE/MERGE predicate labels, deletes, generated SET, and REMOVE match durable SQL rows"
        )
    finally:
        if created_filtered:
            call(f"/graphs/{filtered}", method="DELETE")
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            sql(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
