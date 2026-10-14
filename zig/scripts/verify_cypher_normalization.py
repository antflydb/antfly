#!/usr/bin/env python3
"""Compare native Cypher mutation postimages with durable SQL rows."""

import argparse
import base64
import json
import os
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
    table = "cypher_normalize_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = False

    def call(path, body=None, method="POST"):
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
        assert status in (200, 201, 202, 204), (path, status, content.decode())
        return json.loads(content) if content else None

    def sql(statement):
        return call("/sql", {"statement": statement})

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
        stored = sql(f"SELECT amount,total FROM {table}")
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
            "Native Cypher normalized CREATE, generated SET, alias refresh, and REMOVE match durable SQL rows"
        )
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            sql(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
