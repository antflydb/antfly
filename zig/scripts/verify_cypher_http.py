#!/usr/bin/env python3
"""Exercise production authenticated graph reads against a disposable server.

ANTFLY_CYPHER_TEST_AUTH contains user:password; it is never included in reports.
The campaign creates unique tables and graphs and removes only those fixtures.
"""

import argparse
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    parser.add_argument("--shards", type=int, choices=[1, 2], default=2)
    parser.add_argument("--skip-sql", action="store_true")
    parser.add_argument("--receipt-artifact", type=Path)
    parser.add_argument(
        "--writes",
        action="store_true",
        help="Also qualify production graph transactions",
    )
    args = parser.parse_args()
    auth = os.environ["ANTFLY_CYPHER_TEST_AUTH"]
    header = "Basic " + base64.b64encode(auth.encode()).decode()
    prefix = "cypher_test_" + uuid.uuid4().hex
    table, graph = prefix + "_docs", prefix + "_graph"
    created_table = created_graph = created_reader = created_second_graph = False
    second_graph = prefix + "_second_graph"
    reader = prefix + "_reader"
    reader_password = uuid.uuid4().hex
    checks = 0

    def call(method, path, body=None, authenticated=True, status=None, credential=None):
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = (
                header
                if credential is None
                else "Basic " + base64.b64encode(credential.encode()).decode()
            )
        request = urllib.request.Request(
            args.url.rstrip("/")
            + ("" if path.startswith("/auth/") else "/db/v1")
            + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
        )
        for attempt in range(40):
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    code, content = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, content = error.code, error.read()
            if code == 503 and attempt < 39 and status != 503:
                failure = json.loads(content)
                if failure.get("write_outcome") == "not_started":
                    time.sleep(0.25)
                    continue
            break
        assert (200 <= code < 300) if status is None else code == status, (
            method,
            path,
            code,
            content.decode(),
        )
        if not content:
            return None
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            if code >= 400:
                return content.decode()
            raise

    def query(text, credential=None, **options):
        result = call(
            "POST",
            f"/graphs/{graph}/query",
            {"query": text, **options},
            credential=credential,
        )
        assert result["wire_version"] == 1 and result["complete"] is True
        assert isinstance(result["graph_incarnation"], str)
        assert isinstance(result["graph_revision"], str)
        return result

    def durable_commit(result, credential=None):
        commit = result["commit"]
        if commit["status"] == "committed":
            return True
        if commit["status"] != "committed_pending":
            return False
        # Pending propagation is a durable commit, never a replay invitation.
        receipt = call(
            "GET", f"/transactions/{commit['transaction_id']}", credential=credential
        )
        return receipt["disposition"] == "committed" and receipt["durable"]

    def cell(value, kind, expected):
        assert value == {"type": kind, "value": expected}, value

    def await_read(text):
        """Wait for fresh replicated owners using an explicitly read-only query."""
        deadline = time.monotonic() + 30
        while True:
            try:
                return query(text)
            except AssertionError as error:
                failure = error.args[0]
                if (
                    not isinstance(failure, tuple)
                    or failure[2] != 503
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.2)

    def json_match(min_hops=1, max_hops=1, credential=None, status=None):
        response = call(
            "POST",
            f"/tables/{table}/query",
            {
                "graph_queries": {
                    "matching": {
                        "index": "links",
                        "match": {
                            "anchor": "a",
                            "nodes": {"a": {"filter": {"ids": ["a"]}}, "b": {}},
                            "edges": [
                                {
                                    "from": "a",
                                    "to": "b",
                                    "types": ["cites"],
                                    "min_hops": min_hops,
                                    "max_hops": max_hops,
                                }
                            ],
                        },
                        "return": {"bindings": ["a", "b"], "limit": 10},
                    }
                },
                "limit": 10,
            },
            credential=credential,
            status=status,
        )
        if status is not None:
            return response
        result = response["responses"][0]["graph_results"]["matching"]
        assert result.get("complete", True), result
        return [[row["a"]["key"], row["b"]["key"]] for row in result["rows"]]

    try:
        call(
            "POST",
            f"/tables/{table}",
            {
                "num_shards": args.shards,
                "indexes": {"links": {"type": "graph"}, "text": {"type": "full_text"}},
            },
        )
        created_table = True
        seed = {
            "sync_level": "full_index",
            "inserts": {
                "a": {
                    "id": "a",
                    "title": "αlpha",
                    "_edges": {"links": {"cites": [{"target": "β", "weight": 2}]}},
                },
                "β": {"id": "b", "title": "Beta"},
                "c": {"id": "c", "title": "Gamma"},
            },
        }
        deadline = time.monotonic() + 30
        while True:
            try:
                call("POST", f"/tables/{table}/batch", seed)
                break
            except AssertionError as error:
                # Fresh replicated owners can change certified routes while
                # seeding. Only a proven conflict permits another attempt;
                # transport failures and unknown outcomes remain failures.
                failure = error.args[0]
                if (
                    not isinstance(failure, tuple)
                    or failure[2] != 409
                    or "batch transaction conflicted" not in failure[3]
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.2)
        call(
            "POST",
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
        search = await_read(
            f"CALL antfly.search('{table}', {{query:'Beta',mode:'text',limit:2}}) YIELD node, score RETURN node.id,score"
        )
        assert search["rows"] and search["rows"][0][0]["value"] == "b", search
        assert (
            search["stats"]["search_calls"] == 1
            and search["stats"]["search_candidates"] >= 1
        )
        checks += 1
        selective = call(
            "POST",
            f"/graphs/{graph}/query",
            {
                "query": f"CALL antfly.search('{table}', {{query:'Beta',limit:2}}) YIELD node RETURN node",
                "limits": {"scanned_anchors": 1},
            },
        )
        # Native candidate admission charges the selected posting, rather than
        # enumerating every graph member before ranking.
        assert (
            len(selective["rows"]) == 1 and selective["stats"]["scanned_anchors"] == 1
        ), selective
        call(
            "POST",
            f"/graphs/{graph}/query",
            {
                "query": f"CALL antfly.search('{table}', {{query:'Beta',limit:2}}) YIELD node RETURN node",
                "limits": {"scanned_anchors": 0},
            },
            status=429,
        )
        checks += 1
        rows = query("MATCH (n:Document) RETURN n.id ORDER BY n.id")["rows"]
        assert [row[0]["value"] for row in rows] == ["a", "b", "c"]
        checks += 1
        rows = query(
            "MATCH (a:Document)-[r:CITES]->(b) RETURN a.title,b.title,r.weight"
        )["rows"]
        assert len(rows) == 1
        cell(rows[0][0], "string", "αlpha")
        cell(rows[0][1], "string", "Beta")
        cell(rows[0][2], "number", 2)
        checks += 1
        for hops in [0, 1]:
            cypher_rows = query(
                f"MATCH (a:Document {{id:'a'}})-[:CITES*{hops}..{hops}]->(b) RETURN antfly.documentKey(a),antfly.documentKey(b)"
            )["rows"]
            assert json_match(hops, hops) == [
                [cell["value"] for cell in row] for row in cypher_rows
            ]
            checks += 1
        rows = query(
            "MATCH (a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b) WHERE b.id='b' | b.title]"
        )["rows"]
        assert rows == [
            [{"type": "list", "value": [{"type": "string", "value": "Beta"}]}]
        ], rows
        checks += 1
        rows = query(
            "MATCH (b:Document {id:'b'})<-[r:CITES]-(a) RETURN a.title,r.weight"
        )["rows"]
        assert len(rows) == 1
        cell(rows[0][0], "string", "αlpha")
        cell(rows[0][1], "number", 2)
        checks += 1
        rows = query(
            "MATCH (a:Document {id:'c'}) OPTIONAL MATCH (a)-[:CITES]->(b) RETURN b"
        )["rows"]
        cell(rows[0][0], "null", None)
        checks += 1
        rows = query(
            "MATCH p=(a:Document {id:'c'})-[:CITES*0..0]->(b) RETURN length(p),a=b"
        )["rows"]
        cell(rows[0][0], "integer", "0")
        cell(rows[0][1], "boolean", True)
        checks += 1
        for number in [-(2**63), 2**63 - 1]:
            rows = query(
                "RETURN $n AS n",
                typed_parameters={
                    "type": "map",
                    "value": {"n": {"type": "integer", "value": str(number)}},
                },
            )["rows"]
            cell(rows[0][0], "integer", str(number))
            rows = query("RETURN $n AS n", parameters={"n": number})["rows"]
            cell(rows[0][0], "integer", str(number))
            checks += 2
        result = query("EXPLAIN MATCH (n:Document) RETURN n")
        assert result["rows"] == [] and result["stats"]["scanned_anchors"] == 0
        assert [stage["operation"] for stage in result["plan"]] == ["match", "project"]
        checks += 1
        rows = query(
            "MATCH (n:Document {id:$id}) RETURN n.title", parameters={"id": "b"}
        )["rows"]
        cell(rows[0][0], "string", "Beta")
        checks += 1
        call(
            "POST",
            f"/graphs/{graph}/query",
            {"query": "MATCH (n) RETURN n"},
            authenticated=False,
            status=401,
        )
        checks += 1
        call(
            "POST", f"/graphs/{graph}/query", {"query": "MATCH (n RETURN n"}, status=400
        )
        checks += 1
        if not args.skip_sql:
            statement = f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (n.id AS id)) AS g ORDER BY g.id"
            response = call("POST", "/sql", {"statement": statement})
            assert response["rows"] == [["a"], ["b"], ["c"]]
            checks += 1
            parameterized = f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id=$1 COLUMNS (n.id AS id)) AS g"
            response = call(
                "POST", "/sql", {"statement": parameterized, "parameters": ["b"]}
            )
            assert response["rows"] == [["b"]]
            checks += 1
            joined = f"SELECT g.key, d._id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (antfly.documentKey(n) AS key)) AS g JOIN {table} AS d ON d._id=g.key ORDER BY g.key"
            response = call("POST", "/sql", {"statement": joined})
            assert response["rows"] == [["a", "a"], ["c", "c"], ["β", "β"]]
            checks += 1
            distinct = f"SELECT g.id,h.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS (n.id AS id)) g JOIN GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='b' COLUMNS (n.id AS id)) h ON true"
            response = call("POST", "/sql", {"statement": distinct})
            assert response["rows"] == [["a", "b"]]
            checks += 1
            call(
                "POST",
                f"/graphs/{second_graph}",
                {
                    "labels": [
                        {"name": "Document", "table": table, "where": {"id": "b"}}
                    ]
                },
            )
            created_second_graph = True
            cross_graph = f"SELECT g.id,h.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS (n.id AS id)) g JOIN GRAPH_TABLE({second_graph} MATCH (n IS Document) COLUMNS (n.id AS id)) h ON true"
            response = call("POST", "/sql", {"statement": cross_graph})
            assert response["rows"] == [["a", "b"]]
            checks += 1
            response = call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT g.node FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (n AS node)) g LIMIT 1"
                },
            )
            assert response["rows"][0][0]["type"] == "node"
            checks += 1
            prepared = call("POST", "/sql/prepared", {"statement": parameterized})
            prepared_id = prepared["prepared_id"]
            try:
                assert prepared["parameter_types"] == ["json"]
                response = call(
                    "POST",
                    f"/sql/prepared/{prepared_id}/execute",
                    {"parameters": ["b"]},
                )
                assert response["rows"] == [["b"]]
                checks += 1
            finally:
                call("DELETE", f"/sql/prepared/{prepared_id}")
        call(
            "POST",
            f"/auth/v1/users/{reader}",
            {
                "password": reader_password,
                "initial_policies": [
                    {"resource_type": "graph", "resource": graph, "type": "read"}
                ],
            },
        )
        created_reader = True
        credential = reader + ":" + reader_password
        denied_mapping = prefix + "_denied_mapping"
        call(
            "POST",
            f"/auth/v1/users/{reader}/permissions",
            {"resource_type": "graph", "resource": denied_mapping, "type": "admin"},
        )
        call(
            "POST",
            f"/graphs/{denied_mapping}",
            {"labels": [{"name": "Document", "table": "unreadable_table"}]},
            credential=credential,
            status=403,
        )
        checks += 1
        call(
            "POST",
            f"/graphs/{graph}/query",
            {"query": "MATCH (n) RETURN n"},
            credential=credential,
            status=403,
        )
        checks += 1
        call(
            "POST",
            f"/auth/v1/users/{reader}/permissions",
            {"resource_type": "table", "resource": table, "type": "read"},
        )
        assert (
            len(query("MATCH (n:Document) RETURN n.id", credential=credential)["rows"])
            == 3
        )
        checks += 1
        if not args.skip_sql:
            response = call(
                "POST", "/sql", {"statement": statement}, credential=credential
            )
            assert response["rows"] == [["a"], ["b"], ["c"]]
            checks += 1
            call(
                "POST",
                "/sql",
                {"statement": cross_graph},
                credential=credential,
                status=403,
            )
            checks += 1
        if args.writes:
            call(
                "POST",
                f"/graphs/{graph}/query",
                {
                    "query": "RETURN 'read' AS id UNION ALL CREATE (n:Document {id:'unauthorized_union'}) RETURN n.id AS id"
                },
                credential=credential,
                status=403,
            )
            checks += 1
            call(
                "POST",
                f"/graphs/{graph}/query",
                {"query": "CREATE (n:Document {id:'unauthorized'}) RETURN n"},
                credential=credential,
                status=403,
            )
            call(
                "POST",
                f"/auth/v1/users/{reader}/permissions",
                {"resource_type": "graph", "resource": graph, "type": "write"},
            )
            call(
                "POST",
                f"/graphs/{graph}/query",
                {"query": "CREATE (n:Document {id:'unauthorized'}) RETURN n"},
                credential=credential,
                status=403,
            )
            call(
                "POST",
                f"/auth/v1/users/{reader}/permissions",
                {"resource_type": "table", "resource": table, "type": "write"},
            )
            reader_write_id = uuid.uuid4().hex
            admitted = query(
                "MATCH (n:Document {id:'a'}) SET n.title=n.title RETURN n.id",
                credential=credential,
                write_request_id=reader_write_id,
            )
            assert durable_commit(admitted, credential), admitted
            call(
                "DELETE",
                f"/auth/v1/users/{reader}/permissions?resource={table}&resourceType=table",
            )
            own_receipt = call(
                "GET", f"/transactions/{reader_write_id}/status", credential=credential
            )
            assert (
                own_receipt["disposition"] == "committed" and own_receipt["durable"]
            ), own_receipt
            assert not {
                "tables",
                "read_snapshots",
                "savepoint_ids",
                "execution_plan",
            }.intersection(own_receipt), own_receipt
            call(
                "GET",
                f"/transactions/{reader_write_id}",
                credential=credential,
                status=404,
            )
            call("GET", f"/transactions/{reader_write_id}/status", status=404)
            call(
                "POST",
                f"/auth/v1/users/{reader}/permissions",
                {"resource_type": "table", "resource": table, "type": "read"},
            )
            checks += 3
            call(
                "DELETE",
                f"/auth/v1/users/{reader}/permissions?resource={graph}&resourceType=graph",
            )
            call(
                "POST",
                f"/auth/v1/users/{reader}/permissions",
                {"resource_type": "graph", "resource": graph, "type": "read"},
            )
            checks += 2
        call(
            "POST",
            f"/graphs/{prefix}_other/query",
            {"query": "RETURN 1"},
            credential=credential,
            status=403,
        )
        checks += 1
        call(
            "PUT", f"/auth/v1/users/{reader}/row-filters/{table}", {"term": {"id": "a"}}
        )
        rows = query("MATCH (n:Document) RETURN n.id", credential=credential)["rows"]
        assert len(rows) == 1
        cell(rows[0][0], "string", "a")
        checks += 1
        assert (
            query(
                f"CALL antfly.search('{table}', {{query:'Beta',limit:2}}) YIELD node RETURN node",
                credential=credential,
            )["rows"]
            == []
        )
        checks += 1
        assert (
            query("MATCH (a:Document)-[:CITES]->(b) RETURN b", credential=credential)[
                "rows"
            ]
            == []
        )
        rows = query(
            "MATCH (a:Document) OPTIONAL MATCH (a)-[:CITES]->(b) RETURN b",
            credential=credential,
        )["rows"]
        assert len(rows) == 1
        cell(rows[0][0], "null", None)
        checks += 2
        # The structured matcher requires indexed authorization coverage for
        # this filter. It must refuse rather than return an unfiltered graph;
        # Cypher's owner-side reads above qualify the broader capability.
        unavailable = json_match(credential=credential, status=422)
        assert unavailable["error"] == "graph_anchor_filter_requires_index", unavailable
        checks += 1
        rows = query(
            "MATCH (a:Document) RETURN [(a)-[:CITES]->(b) | b.title]",
            credential=credential,
        )["rows"]
        assert rows == [[{"type": "list", "value": []}]], rows
        checks += 1
        call(
            "DELETE",
            f"/auth/v1/users/{reader}/permissions?resource={graph}&resourceType=graph",
        )
        call(
            "POST",
            f"/graphs/{graph}/query",
            {"query": "RETURN 1"},
            credential=credential,
            status=403,
        )
        checks += 1
        if not args.skip_sql:
            renamed = prefix + "_renamed"
            call(
                "POST",
                "/sql",
                {"statement": f"ALTER TABLE {table} RENAME TO {renamed}"},
            )
            table = renamed
            definition = call("GET", f"/graphs/{graph}")
            assert definition["definition"]["labels"][0]["table"] == renamed
            assert len(query("MATCH (n:Document) RETURN n.id")["rows"]) == 3
            response = call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (n.id AS id)) g ORDER BY g.id"
                },
            )
            assert response["rows"] == [["a"], ["b"], ["c"]]
            checks += 3
        if args.writes:
            write_id = uuid.uuid4().hex
            created = query(
                "CREATE (n:Document {id:'d',title:'Delta',rank:9223372036854775807}) RETURN elementId(n),n.rank",
                write_request_id=write_id,
            )
            assert durable_commit(created), created
            cell(created["rows"][0][1], "integer", "9223372036854775807")
            status = call("GET", f"/transactions/{created['commit']['transaction_id']}")
            assert status["disposition"] == "committed", status
            assert created["commit"]["transaction_id"] == write_id
            if args.receipt_artifact:
                args.receipt_artifact.write_text(
                    json.dumps({"transaction_id": write_id})
                )
            call(
                "POST",
                f"/graphs/{graph}/query",
                {
                    "query": "CREATE (n:Document {id:'duplicate'}) RETURN n",
                    "write_request_id": write_id,
                },
                status=409,
            )
            assert query("MATCH (n:Document {id:'duplicate'}) RETURN n")["rows"] == []
            call("GET", f"/transactions/{write_id}", credential=credential, status=404)
            minimal = call("GET", f"/transactions/{write_id}/status")
            assert minimal["disposition"] == "committed" and minimal["durable"], minimal
            call(
                "GET",
                f"/transactions/{write_id}/status",
                credential=credential,
                status=404,
            )
            checks += 4

            def concurrent_merge(_):
                request_id = uuid.uuid4().hex
                result = call(
                    "POST",
                    f"/graphs/{graph}/query",
                    {
                        "query": "MERGE (n:Document {id:'concurrent'}) RETURN elementId(n)",
                        "write_request_id": request_id,
                    },
                    status=None,
                )
                if result["commit"]["status"] == "unknown":
                    deadline = time.monotonic() + 30
                    while True:
                        try:
                            receipt = call("GET", f"/transactions/{request_id}")
                        except AssertionError as error:
                            failure = error.args[0]
                            if not isinstance(failure, tuple) or failure[2] not in (
                                404,
                                503,
                            ):
                                raise
                            receipt = {"disposition": "outcome_unknown"}
                        if receipt["disposition"] in ("committed", "aborted"):
                            assert receipt["durable"], receipt
                            return (
                                result["rows"][0][0]
                                if receipt["disposition"] == "committed"
                                else None
                            )
                        assert time.monotonic() < deadline, receipt
                        time.sleep(0.25)
                assert durable_commit(result), result
                return result["rows"][0][0]

            # Conflicts are definite aborts; an unknown commit must never be retried.
            def merge_attempt(index):
                try:
                    return concurrent_merge(index)
                except AssertionError as error:
                    failure = error.args[0]
                    if isinstance(failure, tuple) and (
                        (
                            failure[2] == 409
                            and "CypherTransactionConflict" in failure[3]
                        )
                        or (
                            failure[2] == 429
                            and json.loads(failure[3]).get("write_outcome")
                            == "not_started"
                            and json.loads(failure[3]).get("error")
                            == "SqlWriteCapacityUnavailable"
                        )
                        or (
                            failure[2] == 503
                            and json.loads(failure[3]).get("write_outcome")
                            == "not_started"
                            and json.loads(failure[3]).get("error")
                            in ("Backpressured", "ReadUnavailable", "NotLeader")
                        )
                    ):
                        return None
                    raise

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                merged_ids = list(pool.map(merge_attempt, range(4)))
            assert any(value is not None for value in merged_ids), merged_ids
            no_op_id = uuid.uuid4().hex
            no_op = query(
                "MERGE (n:Document {id:'concurrent'}) RETURN elementId(n)",
                write_request_id=no_op_id,
            )
            assert no_op["stats"]["mutations"] == 0 and durable_commit(no_op), no_op
            no_op_receipt = call("GET", f"/transactions/{no_op_id}")
            assert no_op_receipt["disposition"] == "committed", no_op_receipt
            call(
                "POST",
                f"/graphs/{graph}/query",
                {
                    "query": "MERGE (n:Document {id:'concurrent'}) RETURN elementId(n)",
                    "write_request_id": no_op_id,
                },
                status=409,
            )
            checks += 3
            assert (
                len(query("MATCH (n:Document {id:'concurrent'}) RETURN n")["rows"]) == 1
            )
            assert (
                len(
                    {
                        json.dumps(value, sort_keys=True)
                        for value in merged_ids
                        if value is not None
                    }
                )
                == 1
            )
            checks += 2
            edge = query(
                "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:CITES {weight:9223372036854775807,nested:{type:'node'}}]->(b) RETURN elementId(r),r.weight"
            )
            edge_id = edge["rows"][0][0]["value"]
            cell(edge["rows"][0][1], "integer", "9223372036854775807")
            checks += 1
            rows = query(
                "MATCH (a:Document {id:'a'})-[r:CITES]->(b:Document) RETURN r.weight ORDER BY r.weight"
            )["rows"]
            assert len(rows) == 2, rows
            cell(rows[1][0], "integer", "9223372036854775807")
            checks += 1
            assert query(
                "MATCH (n:Document {id:'d'}) SET n.title=null REMOVE n.absent RETURN n.title"
            )["rows"] == [[{"type": "null", "value": None}]]
            checks += 1
            merged = query(
                "MERGE (n:Document {id:'d'}) ON MATCH SET n.title='Updated' RETURN elementId(n)"
            )
            assert merged["rows"][0][0] == created["rows"][0][0]
            assert len(query("MATCH (n:Document {id:'d'}) RETURN n")["rows"]) == 1
            checks += 1
            call(
                "POST",
                f"/graphs/{graph}/query",
                {"query": "CREATE (n:Document {id:'failed'}) RETURN 1/0"},
                status=400,
            )
            assert query("MATCH (n:Document {id:'failed'}) RETURN n")["rows"] == []
            checks += 1
            call(
                "POST",
                f"/graphs/{graph}/query",
                {
                    "query": "MATCH (a:Document {id:'a'}) SET a.title='must rollback' DELETE a RETURN a"
                },
                status=400,
            )
            cell(
                query("MATCH (a:Document {id:'a'}) RETURN a.title")["rows"][0][0],
                "string",
                "αlpha",
            )
            checks += 1
            query(
                "MATCH ()-[r:CITES]->() WHERE elementId(r)=$edge DELETE r",
                parameters={"edge": edge_id},
            )
            assert (
                len(query("MATCH (:Document {id:'a'})-[r:CITES]->() RETURN r")["rows"])
                == 1
            )
            checks += 1
            query("MATCH (n:Document {id:'d'}) DETACH DELETE n")
            assert query("MATCH (n:Document {id:'d'}) RETURN n")["rows"] == []
            checks += 1
            union = query(
                "RETURN 'before' AS id UNION ALL CREATE (n:Document {id:'union_created'}) RETURN n.id AS id UNION ALL MATCH (n:Document {id:'union_created'}) RETURN n.id AS id"
            )
            assert durable_commit(union) and not union["read_only"], union
            assert union["rows"] == [
                [{"type": "string", "value": value}]
                for value in ["before", "union_created", "union_created"]
            ], union
            checks += 1
            call(
                "POST",
                f"/graphs/{graph}/query",
                {
                    "query": "CREATE (n:Document {id:'union_aborted'}) RETURN n.id AS id UNION ALL RETURN 1/0 AS id"
                },
                status=400,
            )
            assert (
                query("MATCH (n:Document {id:'union_aborted'}) RETURN n")["rows"] == []
            )
            checks += 1
        print(
            f"Authenticated Cypher HTTP campaign ({args.shards} owners): {checks} checks passed"
        )
    finally:
        if created_reader:
            call("DELETE", f"/auth/v1/users/{reader}")
        if created_second_graph:
            call("DELETE", f"/graphs/{second_graph}")
        if created_graph:
            call("DELETE", f"/graphs/{graph}")
        if created_table:
            call("DELETE", f"/tables/{table}")


if __name__ == "__main__":
    main()
