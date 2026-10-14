#!/usr/bin/env python3
"""Qualify authenticated graph definition revisions against a native server."""

import argparse
import base64
import concurrent.futures
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
    admin = os.environ["ANTFLY_CYPHER_TEST_AUTH"]
    prefix = "cypher_catalog_" + uuid.uuid4().hex
    tables = [prefix + "_a", prefix + "_b"]
    graph, reader = prefix + "_graph", prefix + "_reader"
    password = uuid.uuid4().hex
    created_tables = []
    created_graph = created_reader = False
    prepared_id = None
    checks = 0

    def call(
        method, path, body=None, *, credential=admin, status=None, retry_aborted=False
    ):
        request = urllib.request.Request(
            args.url.rstrip("/")
            + ("" if path.startswith("/auth/") else "/db/v1")
            + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Basic "
                + base64.b64encode(credential.encode()).decode(),
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, raw = error.code, error.read()
        if retry_aborted and code == 409 and raw == b"batch transaction conflicted":
            # This response is a definite prepare abort, never an unknown outcome.
            for _ in range(30):
                time.sleep(0.2)
                try:
                    return call(
                        method, path, body, credential=credential, status=status
                    )
                except AssertionError as retry_error:
                    failure = retry_error.args[0]
                    if failure[2:] != (409, "batch transaction conflicted"):
                        raise
        assert (200 <= code < 300) if status is None else code == status, (
            method,
            path,
            code,
            raw.decode(),
        )
        return json.loads(raw) if raw else None

    def definition(table, where=None):
        label = {"name": "Document", "table": table}
        if where is not None:
            label["where"] = where
        return {"labels": [label]}

    def update(revision, mapping, **kwargs):
        return call(
            "PUT",
            f"/graphs/{graph}",
            {"expected_revision": revision, "definition": mapping},
            **kwargs,
        )

    def query():
        return call(
            "POST",
            f"/graphs/{graph}/query",
            {"query": "MATCH (n:Document) RETURN n.id ORDER BY n.id"},
        )

    try:
        for table in tables:
            call("POST", f"/tables/{table}", {"num_shards": 1})
            created_tables.append(table)
            call(
                "POST",
                f"/tables/{table}/batch",
                {
                    "sync_level": "full_index",
                    "inserts": {"a": {"id": "a"}, "b": {"id": "b"}},
                },
                retry_aborted=True,
            )
        initial = call("POST", f"/graphs/{graph}", definition(tables[0]))
        created_graph = True
        assert initial["revision"] == "1", initial
        incarnation = initial["graph_id"]
        readiness = call(
            "POST", f"/graphs/{graph}/query", {"query": "MATCH (n) RETURN n LIMIT 0"}
        )
        assert readiness["complete"] and readiness["rows"] == [], readiness
        prepared = call(
            "POST",
            "/sql/prepared",
            {
                "statement": f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS(n.id AS id)) g"
            },
        )
        prepared_id = prepared["prepared_id"]
        assert (
            len(
                call(
                    "POST", f"/sql/prepared/{prepared_id}/execute", {"parameters": []}
                )["rows"]
            )
            == 2
        )
        checks += 1
        revised = update("1", definition(tables[0], {"id": "b"}))
        assert revised["revision"] == "2" and revised["graph_id"] == incarnation
        result = query()
        assert result["graph_revision"] == "2"
        assert result["graph_incarnation"] == str(incarnation)
        assert result["rows"] == [[{"type": "string", "value": "b"}]], result
        checks += 1
        rejected = update("1", definition(tables[1]), status=409)
        assert rejected["code"] == "CatalogGenerationChanged", rejected
        assert call("GET", f"/graphs/{graph}")["revision"] == "2"
        for malformed in ["0", "01", "+2", "18446744073709551616"]:
            update(malformed, definition(tables[0]), status=400)
        checks += 1
        invalidated = call(
            "POST",
            f"/sql/prepared/{prepared_id}/execute",
            {"parameters": []},
            status=409,
        )
        assert invalidated["code"] == "40001", invalidated
        checks += 1
        call(
            "POST",
            f"/auth/v1/users/{reader}",
            {
                "password": password,
                "initial_policies": [
                    {"resource_type": "graph", "resource": graph, "type": "admin"},
                    {"resource_type": "table", "resource": tables[0], "type": "read"},
                ],
            },
        )
        created_reader = True
        update(
            "2", definition(tables[1]), credential=reader + ":" + password, status=403
        )
        assert call("GET", f"/graphs/{graph}")["revision"] == "2"
        checks += 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

            def competing(mapping):
                try:
                    return update("2", mapping)["revision"]
                except AssertionError as error:
                    failure = error.args[0]
                    assert (
                        failure[2] == 409 and "CatalogGenerationChanged" in failure[3]
                    ), failure
                    return "conflict"

            results = list(
                executor.map(competing, [definition(tables[0]), definition(tables[1])])
            )
        assert sorted(results) == ["3", "conflict"], results
        assert call("GET", f"/graphs/{graph}")["revision"] == "3"
        assert len(query()["rows"]) == 2
        checks += 1
        print(f"{checks} authenticated graph catalog revision checks passed")
    finally:
        if prepared_id is not None:
            call("DELETE", f"/sql/prepared/{prepared_id}")
        if created_reader:
            call("DELETE", f"/auth/v1/users/{reader}")
        if created_graph:
            call("DELETE", f"/graphs/{graph}")
        for table in reversed(created_tables):
            call("DELETE", f"/tables/{table}")


if __name__ == "__main__":
    main()
