#!/usr/bin/env python3
"""Check Cypher postimage reads when USING is narrower than WITH CHECK."""

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
    parser.add_argument("--shards", type=int, choices=[1, 2], default=2)
    args = parser.parse_args()
    authorization = (
        "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode()
    )
    table = "cypher_postimage_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = enabled = False
    checks = 0

    def call(
        path,
        body=None,
        *,
        method="POST",
        accepted=(200, 201, 202, 204),
        retry_aborted=False,
    ):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, raw = error.code, error.read()
        if retry_aborted and code == 409 and raw == b"batch transaction conflicted":
            # Fresh owners can reject prepare while their routes converge.
            # Only the explicit noncommit response permits fixture replay.
            for _ in range(30):
                time.sleep(0.2)
                try:
                    return call(path, body, method=method, accepted=accepted)
                except AssertionError as retry_error:
                    failure = retry_error.args[0]
                    if failure[1:] != (409, "batch transaction conflicted"):
                        raise
        assert code in accepted, (path, code, raw.decode())
        return json.loads(raw) if raw else None

    def sql(statement):
        return call("/sql", {"statement": statement})

    def query(statement, *, accepted=(200,)):
        return call(f"/graphs/{graph}/query", {"query": statement}, accepted=accepted)

    def write(statement):
        result = query(statement, accepted=(200, 202))
        receipt = result["commit"]
        if receipt["status"] != "committed":
            # An admitted write is never replayed. Resolve its durable receipt
            # before issuing another statement against the changed rows.
            deadline = time.monotonic() + 90
            while True:
                status = call(
                    f"/transactions/{receipt['transaction_id']}/status",
                    method="GET",
                    accepted=(200, 409, 503),
                )
                disposition = status.get("disposition")
                assert disposition != "aborted", status
                if disposition == "committed" and status.get("durable"):
                    break
                assert time.monotonic() < deadline, status
                time.sleep(0.2)
            receipt["status"] = "committed"
        return result

    null = {"type": "null", "value": None}
    try:
        call(
            f"/tables/{table}",
            {
                "num_shards": args.shards,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    name: {"type": "string", "nullable": True}
                                    for name in ["id", "tenant", "body"]
                                },
                            }
                        }
                    },
                },
                "indexes": {"links": {"type": "graph"}},
            },
        )
        created_table = True
        call(
            f"/tables/{table}/batch",
            {
                "sync_level": "full_index",
                "inserts": {
                    name: {"id": name, "tenant": tenant, "body": name}
                    for name, tenant in [
                        ("a", "visible"),
                        ("b", "visible"),
                        ("h", "hidden"),
                    ]
                },
            },
            retry_aborted=True,
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
        sql(
            f"CREATE POLICY visible ON {table} USING (tenant='visible') WITH CHECK (true)"
        )
        sql(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        enabled = True
        deadline = time.monotonic() + 90
        while True:
            visible = query(
                "MATCH (n:Document) RETURN n.id ORDER BY n.id",
                accepted=(200, 403, 409, 503),
            )
            if visible.get("rows") == [
                [{"type": "string", "value": "a"}],
                [{"type": "string", "value": "b"}],
            ]:
                break
            assert time.monotonic() < deadline, visible
            time.sleep(0.2)
        hidden = write(
            "CREATE (n:Document {id:'created_hidden',tenant:'hidden'}) RETURN n AS node,n.tenant AS tenant,keys(n) AS fields,n{.*} AS properties"
        )
        assert hidden["commit"]["status"] == "committed", hidden
        assert hidden["rows"] == [[null, null, null, null]], hidden
        checks += 1
        changed = write(
            "MATCH (n:Document {id:'a'}) SET n.tenant='hidden' WITH n MATCH (m:Document {id:'a'}) RETURN m"
        )
        assert changed["commit"]["status"] == "committed" and changed["rows"] == [], (
            changed
        )
        checks += 1
        aliases = write(
            "MATCH (n:Document {id:'b'}) WITH n AS a,n AS b SET a.tenant='hidden' RETURN b AS node,b.tenant AS tenant,[b] AS nodes,{copy:b} AS wrapped"
        )
        assert aliases["commit"]["status"] == "committed", aliases
        assert aliases["rows"] == [
            [
                null,
                null,
                {"type": "list", "value": [null]},
                {"type": "map", "value": {"copy": null}},
            ]
        ], aliases
        checks += 1
        sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        enabled = False
        expected = [
            ["a", "hidden"],
            ["b", "hidden"],
            ["created_hidden", "hidden"],
            ["h", "hidden"],
        ]
        deadline = time.monotonic() + 90
        while True:
            result = query(
                "MATCH (n:Document) RETURN n.id,n.tenant ORDER BY n.id",
                accepted=(200, 403, 409, 503),
            )
            if [
                [cell["value"] for cell in row] for row in result.get("rows", [])
            ] == expected:
                break
            assert time.monotonic() < deadline, result
            time.sleep(0.2)
        deadline = time.monotonic() + 90
        while True:
            durable = call(
                "/sql",
                {"statement": f"SELECT id,tenant FROM {table} ORDER BY id"},
                accepted=(200, 409, 503),
            )
            if durable.get("rows") == expected:
                break
            assert time.monotonic() < deadline, durable
            time.sleep(0.2)
        checks += 1
        print(
            f"{checks} authenticated Cypher postimage-policy checks passed on {args.shards} shards"
        )
    finally:
        if enabled:
            sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call(f"/tables/{table}", method="DELETE")


if __name__ == "__main__":
    main()
