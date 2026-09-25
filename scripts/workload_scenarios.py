"""Explicit API scenarios with deterministic offered mixes and checked results.

Plans are data, never executable shell/Python. Fixture writes are not retried.
A deliberate stream disconnect is evidence of cancellation, never useful throughput.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import math
import re
import time
from pathlib import Path


def validate(workload):
    operations = workload.get("operations", [])
    if not operations or len(operations) > 64:
        raise ValueError("scenario needs 1..64 explicit operations")
    if len(workload.get("setup", [])) > 10000:
        raise ValueError("scenario setup exceeds 10000 requests")
    classes = set()
    for operation in operations + workload.get("setup", []):
        if operation.get("method") not in {"GET", "POST", "PUT", "DELETE"}:
            raise ValueError("explicit HTTP method required")
        path = operation.get("path", "")
        if not path.startswith("/db/v1/") or "\r" in path or "\n" in path:
            raise ValueError("scenario paths must use the local /db/v1/ API")
        expect = operation.get("expect", {})
        if type(expect.get("status")) is not int or not 200 <= expect["status"] < 300:
            raise ValueError("successful status must be declared exactly")
        checks = expect.get("checks", [])
        stream = operation.get("stream")
        if not checks and not stream:
            raise ValueError("response needs explicit semantic checks")
        for check in checks:
            if not isinstance(check.get("path"), list) or set(check) not in (
                {"path", "equals"},
                {"path", "length"},
                {"path", "sorted_equals"},
            ):
                raise ValueError("checks require a JSON path and exact value/length")
        if len(json.dumps(operation.get("body")).encode()) > 16 << 20:
            raise ValueError("scenario body exceeds 16 MiB")
        if stream:
            if operation["method"] != "GET" or not path.split("?", 1)[0].endswith(
                "/keys"
            ):
                raise ValueError("stream mode is restricted to the scan keys endpoint")
            if stream.get("mode") not in {"drain", "disconnect"}:
                raise ValueError("stream mode must be drain or disconnect")
            for field, low, high in (
                ("chunk_bytes", 1, 65536),
                ("max_bytes", 1, 64 << 20),
                ("pause_seconds", 0, 30),
            ):
                value = stream.get(field)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or not low <= value <= high
                ):
                    raise ValueError(f"invalid stream {field}")
            if (
                type(stream["chunk_bytes"]) is not int
                or type(stream["max_bytes"]) is not int
            ):
                raise ValueError("stream byte bounds must be integers")
            if stream["mode"] == "drain" and not re.fullmatch(
                r"[a-f0-9]{64}", stream.get("sha256", "")
            ):
                raise ValueError("drained stream needs a predeclared exact body digest")
        if operation in operations:
            kind = operation.get("class", "")
            if not re.fullmatch(r"[a-z][a-z0-9_]*", kind) or kind in classes:
                raise ValueError("scenario classes must be unique")
            classes.add(kind)
            if (
                type(operation.get("weight")) is not int
                or not 1 <= operation["weight"] <= 100
            ):
                raise ValueError("scenario weight must be an integer 1..100")
            if type(operation.get("is_write")) is not bool:
                raise ValueError("declare write ambiguity for each scenario operation")
            read_path = path.split("?", 1)[0]
            proven_read = operation["method"] == "GET" or (
                operation["method"] == "POST" and read_path.endswith("/query")
            )
            if not operation["is_write"] and not proven_read:
                raise ValueError(
                    "non-read endpoints must retain unknown write outcomes"
                )


def select(workload, sequence):
    operations = workload["operations"]
    # A coprime walk interleaves classes instead of emitting long write bursts.
    total = sum(item["weight"] for item in operations)
    stride = next(
        value
        for value in range(max(1, total // 2), total + 1)
        if math.gcd(value, total) == 1
    )
    cursor = (sequence * stride) % total
    for index, operation in enumerate(operations):
        if cursor < operation["weight"]:
            return index, operation
        cursor -= operation["weight"]
    raise AssertionError("unreachable scenario selection")


def classify(operation, status, body):
    if status == 429:
        return "rejected"
    if status != operation["expect"]["status"]:
        return "unexpected_http_error"
    try:
        value = json.loads(body)
        if isinstance(value, dict) and (
            value.get("error")
            or any(row.get("error") for row in value.get("responses", []))
        ):
            return "invalid_result"
        for check in operation["expect"].get("checks", []):
            selected = value
            for component in check["path"]:
                selected = selected[component]
            if "equals" in check and selected != check["equals"]:
                return "invalid_result"
            if "length" in check and len(selected) != check["length"]:
                return "invalid_result"
            if "sorted_equals" in check and sorted(selected) != sorted(
                check["sorted_equals"]
            ):
                return "invalid_result"
        return "completed"
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return "invalid_result"


def seed(http, port, workload, directory):
    receipts = []
    client = http(port, 30)
    try:
        for index, operation in enumerate(workload.get("setup", [])):
            status, body, _ = client.request(
                operation["method"], operation["path"], operation.get("body")
            )
            outcome = classify(operation, status, body)
            receipts.append(
                {
                    "step": index,
                    "status": status,
                    "outcome": outcome,
                    "body": body[:16384].decode(errors="replace"),
                }
            )
            if outcome != "completed":
                raise RuntimeError(
                    f"scenario fixture {workload['name']} step {index}: {outcome}; write not replayed"
                )
    finally:
        client.close()
        (directory / f"{workload['name']}-fixture.json").write_text(
            json.dumps(receipts, indent=2) + "\n"
        )


def stream_request(port, operation, deadline):
    """Absolute original submission deadline covers connect, reads and pauses."""
    spec = operation["stream"]
    connection = http.client.HTTPConnection("127.0.0.1", port)
    size, digest = 0, hashlib.sha256()

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError("original stream deadline expired")
        connection.timeout = budget
        if connection.sock:
            connection.sock.settimeout(budget)
        return budget

    try:
        remaining()
        connection.request(operation["method"], operation["path"])
        response = connection.getresponse()
        if response.status != operation["expect"]["status"]:
            return {
                "status": response.status,
                "outcome": (
                    "rejected" if response.status == 429 else "unexpected_http_error"
                ),
                "stream_bytes": size,
            }
        while True:
            remaining()
            chunk = response.read1(
                min(spec["chunk_bytes"], spec["max_bytes"] + 1 - size)
            )
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
            if size > spec["max_bytes"]:
                raise ValueError("stream exceeded declared byte bound")
            pause = spec["pause_seconds"]
            time.sleep(min(pause, remaining()))
            remaining()
            if spec["mode"] == "disconnect":
                return {
                    "status": response.status,
                    "outcome": "intentional_disconnect",
                    "stream_bytes": size,
                }
        valid = spec["mode"] == "drain" and digest.hexdigest() == spec["sha256"]
        return {
            "status": response.status,
            "outcome": "completed" if valid else "invalid_result",
            "stream_bytes": size,
            "stream_sha256": digest.hexdigest(),
        }
    finally:
        connection.close()


def mixed_fixture(rows=4096, read_percent=90, graph_depth=32):
    """Separate deterministic chain/text/aggregation fixture; no benchmark substitution.

    Request shapes follow zig/e2e/antfly/test_graph.py,
    test_query_string.py and test_index_lifecycle.py. Duration depends on the
    measured machine; a query is never declared 'long' from its label alone.
    """
    if (
        not 2 <= rows <= 100000
        or not 1 <= graph_depth < rows
        or read_percent not in (90, 50)
    ):
        raise ValueError("bounded rows/depth and 90/10 or 50/50 mix required")
    name = f"operators{read_percent}"
    table = f"workload_{name}"
    root = f"/db/v1/tables/{table}"

    def check(path, value):
        return {"path": path, "equals": value}

    def request(method, path, body, checks, status=200):
        return {
            "method": method,
            "path": path,
            "body": body,
            "expect": {"status": status, "checks": checks},
        }

    def document(index):
        value = {
            "marker": f"row{index}",
            "body": "needle unique" if index == 0 else "background common",
            "amount": index,
        }
        if index + 1 < rows:
            value["_edges"] = {"graph_idx": {"cites": [{"target": f"row{index + 1}"}]}}
        return value

    setup = [
        request("POST", root, {"num_shards": 1}, [check(["name"], table)], 200),
        request(
            "POST",
            root + "/indexes/graph_idx",
            {"type": "graph", "edge_types": [{"name": "cites"}]},
            [check(["name"], "graph_idx"), check(["type"], "graph")],
            201,
        ),
    ]
    for first in range(0, rows, 128):
        inserts = {
            f"row{index}": document(index)
            for index in range(first, min(rows, first + 128))
        }
        setup.append(
            request(
                "POST",
                root + "/batch",
                {"inserts": inserts, "sync_level": "full_index"},
                [check(["inserted"], len(inserts))],
                201,
            )
        )
    lookup = request("GET", root + "/documents/row0", None, [check(["marker"], "row0")])
    text = request(
        "POST",
        root + "/query",
        {
            "full_text_search": {"match": {"field": "body", "text": "needle"}},
            "fields": [],
            "limit": 2,
        },
        [
            check(["responses", 0, "hits", "hits", 0, "_id"], "row0"),
            {"path": ["responses", 0, "hits", "hits"], "length": 1},
        ],
    )
    aggregate = request(
        "POST",
        root + "/query",
        {
            "limit": 0,
            "aggregations": {"amount_sum": {"type": "sum", "field": "amount"}},
        },
        [
            check(
                ["responses", 0, "aggregations", "amount_sum", "value"],
                rows * (rows - 1) // 2,
            )
        ],
    )
    graph = request(
        "POST",
        root + "/query",
        {
            "limit": 0,
            "graph_queries": {
                "walk": {
                    "index": "graph_idx",
                    "traverse": {
                        "start": {"keys": ["row0"]},
                        "edge_types": ["cites"],
                        "max_depth": graph_depth,
                    },
                }
            },
        },
        [
            {
                "path": ["responses", 0, "graph_results", "walk", "nodes"],
                "length": graph_depth,
            }
        ],
    )
    write = request(
        "POST",
        root + "/batch",
        {"inserts": {"row0": document(0)}, "sync_level": "full_index"},
        [check(["inserted"], 1)],
        201,
    )
    # Preflight every query against the seeded ground truth, never warm up a
    # malformed/partial query and then call its fast failures performance.
    setup.extend([lookup, text, aggregate, graph])
    weights = [read_percent // 4] * 4
    weights[0] += read_percent - sum(weights)
    operations = [
        {**item, "class": kind, "weight": weight, "is_write": kind == "write"}
        for item, kind, weight in zip(
            [lookup, text, aggregate, graph, write],
            ["lookup", "selective_text", "aggregation", "graph", "write"],
            [*weights, 100 - read_percent],
        )
    ]
    return {
        "kind": "scenario",
        "name": name,
        "fixture_provenance": "separate deterministic chain/text/aggregation fixture; not retained vector or graph benchmark",
        "rows": rows,
        "graph_depth": graph_depth,
        "setup": setup,
        "operations": operations,
    }
