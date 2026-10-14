#!/usr/bin/env python3
"""Qualify expression compatibility against authenticated native SQL storage."""

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
    table = "cypher_expressions_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = False
    checks = 0

    def call(path, body=None, *, method="POST", status=None, retry_aborted=False):
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
        if retry_aborted and code == 409:
            failure = json.loads(raw)
            # Only the explicit noncommit diagnostic permits fixture replay.
            # An unknown outcome (40003) must never take this path.
            if failure.get("code") == "40001" and "was not committed" in failure.get(
                "message", ""
            ):
                for _ in range(30):
                    time.sleep(0.2)
                    try:
                        return call(path, body, method=method, status=status)
                    except AssertionError as retry_error:
                        detail = retry_error.args[0]
                        if not isinstance(detail, tuple) or detail[1] != 409:
                            raise
                        diagnostic = json.loads(detail[2])
                        if diagnostic.get(
                            "code"
                        ) != "40001" or "was not committed" not in diagnostic.get(
                            "message", ""
                        ):
                            raise
        assert (200 <= code < 300) if status is None else code == status, (
            path,
            code,
            raw.decode(),
        )
        return json.loads(raw) if raw else None

    def query(statement, **options):
        result = call(f"/graphs/{graph}/query", {"query": statement, **options})
        assert result["complete"], result
        return result

    def strings(result):
        return [[value["value"] for value in row] for row in result["rows"]]

    try:
        call(
            "/sql",
            {
                "statement": f"CREATE TABLE {table} (id TEXT,title TEXT,rank BIGINT,note TEXT,payload JSONB)"
            },
        )
        created_table = True
        call(
            "/sql",
            {
                "statement": f"INSERT INTO {table} (id,title,rank,payload) VALUES ('a','Alpha',1,'{{\"value\":null}}'::JSONB),('b','Beta',2,NULL),('c','Gamma',3,NULL)"
            },
            retry_aborted=True,
        )
        call(f"/graphs/{graph}", {"labels": [{"name": "Document", "table": table}]})
        created_graph = True
        assert strings(
            query(
                "MATCH (n:Document) WITH n.title AS title WHERE n.rank>1 RETURN title ORDER BY title"
            )
        ) == [["Beta"], ["Gamma"]]
        checks += 1
        assert strings(
            query("MATCH (n:Document) WHERE n.title =~ '(?i)a.*' RETURN n.id")
        ) == [["a"]]
        checks += 1
        assert strings(
            query(
                "MATCH (n:Document) WHERE n.title =~ $pattern RETURN n.id ORDER BY n.id",
                parameters={"pattern": "Beta|Gamma"},
            )
        ) == [["b"], ["c"]]
        checks += 1
        assert query("RETURN $value =~ '.*'", parameters={"value": 1})["rows"] == [
            [{"type": "null", "value": None}]
        ]
        checks += 1
        properties = query(
            "MATCH (n:Document {id:'a'}) RETURN keys(n),n.note,n.payload"
        )["rows"][0]
        assert "note" not in {value["value"] for value in properties[0]["value"]}, (
            properties
        )
        assert properties[1] == {"type": "null", "value": None}, properties
        assert properties[2] == {
            "type": "map",
            "value": {"value": {"type": "null", "value": None}},
        }, properties
        checks += 1
        invalid = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:Document {id:'missing'}) WHERE n.title =~ '(' RETURN n"
            },
            status=400,
        )
        assert invalid["code"] == "CypherInvalidRegex", invalid
        checks += 1
        limited = call(
            f"/graphs/{graph}/query",
            {"query": "RETURN 'abc' =~ 'a.*'", "limits": {"intermediate_states": 10}},
            status=429,
        )
        assert limited["code"] == "CypherStateBudgetExceeded", limited
        checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS(n.id AS id,n.title AS title)) g WHERE g.title ~ '^A'"
            },
        )
        assert sql["rows"] == [["a"]], sql
        checks += 1
        print(f"{checks} native Cypher/SQL expression checks passed")
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call("/sql", {"statement": f"DROP TABLE {table}"})


if __name__ == "__main__":
    main()
