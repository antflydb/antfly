#!/usr/bin/env python3
"""Verify production writes above 65,536 rows and configured budget rejection.

Start the server with max_intermediate_states >= 100000 and
max_retained_state_bytes >= 2147483648. The fixture cleans up its table/graph.
"""

import argparse
import base64
import json
import os
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18594")
    args = parser.parse_args()
    headers = {"Content-Type": "application/json"}
    if credentials := os.environ.get("ANTFLY_CYPHER_TEST_AUTH"):
        headers["Authorization"] = (
            "Basic " + base64.b64encode(credentials.encode()).decode()
        )
    name = "cypher_limits_" + uuid.uuid4().hex
    table_created = graph_created = False

    def call(path, body=None, method="POST", expected_error=None):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                content = response.read()
                assert expected_error is None, "expected budget rejection"
                return json.loads(content) if content else None
        except urllib.error.HTTPError as error:
            content = error.read().decode()
            assert expected_error and expected_error in content, (error.code, content)
            return None

    def query(statement, states=100000, expected_error=None, parameters=None):
        return call(
            f"/graphs/{name}/query",
            {
                "query": statement,
                "parameters": parameters or {},
                "limits": {"memory_bytes": 2147483648, "intermediate_states": states},
            },
            expected_error=expected_error,
        )

    def count(expected):
        result = call("/sql", {"statement": f"SELECT count(*) FROM {name}"})
        assert result["rows"] == [[str(expected)]], result

    try:
        call(f"/tables/{name}", {"num_shards": 1, "indexes": {}})
        table_created = True
        for start in range(0, 65537, 4096):
            call(
                f"/tables/{name}/batch",
                {
                    "sync_level": "full_index",
                    "inserts": {
                        f"k{i:06d}": {} for i in range(start, min(65537, start + 4096))
                    },
                },
            )
        call(f"/graphs/{name}", {"labels": [{"name": "Document", "table": name}]})
        graph_created = True
        query(
            "CREATE (n:Document {amount:1}) RETURN n.amount",
            65536,
            "CypherStateBudgetExceeded",
        )
        count(65537)
        result = query(
            "CREATE (n:Document {amount:1}) RETURN n.amount,antfly.documentKey(n)"
        )
        assert result["commit"]["status"] == "committed", result
        assert result["rows"][0][0] == {"type": "integer", "value": "1"}, result
        count(65538)
        # Delete a row that existed at the retained cut, without row predicates.
        query(
            "MATCH (n:Document) WHERE antfly.documentKey(n)=$key DELETE n",
            parameters={"key": result["rows"][0][1]["value"]},
        )
        count(65537)
        # A node created then deleted has no durable preimage to delete.
        query("CREATE (n:Document) DELETE n")
        count(65537)
        print(
            "Production writes above 65,536 rows, preimage deletes, transient creates, and configured limits passed"
        )
    finally:
        if graph_created:
            call(f"/graphs/{name}", method="DELETE")
        if table_created:
            call(f"/tables/{name}", method="DELETE")


if __name__ == "__main__":
    main()
