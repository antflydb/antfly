#!/usr/bin/env python3
"""Verify that an Antfly schema migration keeps health, status, and search live."""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


def request_json(method: str, url: str, body: bytes | None, timeout: float) -> tuple[int, object, float]:
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"content-type": "application/json"} if body is not None else {},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else None, time.monotonic() - started
    except urllib.error.HTTPError as error:
        payload = error.read()
        try:
            decoded: object = json.loads(payload) if payload else None
        except json.JSONDecodeError:
            decoded = payload.decode("utf-8", errors="replace")
        return error.code, decoded, time.monotonic() - started


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--table", default="antfly-benchmark")
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--expected-index", required=True)
    parser.add_argument("--query-term", default="alpha")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--interval-seconds", type=float, default=0.05)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    table_url = f"{base_url}/tables/{args.table}"
    schema_url = f"{table_url}/schema"
    query_url = f"{table_url}/query"
    schema_body = args.schema.read_bytes()
    query_body = json.dumps(
        {
            "full_text_search": {"match": args.query_term, "field": "body"},
            "fields": ["_id"],
            "limit": 1,
            "count": True,
        },
        separators=(",", ":"),
    ).encode()

    update: dict[str, object] = {}

    def update_schema() -> None:
        try:
            status, payload, elapsed = request_json(
                "PUT", schema_url, schema_body, args.timeout_seconds
            )
            update.update(status=status, payload=payload, elapsed_seconds=elapsed)
        except BaseException as error:  # Preserve the worker failure in the result.
            update["error"] = repr(error)

    worker = threading.Thread(target=update_schema, daemon=True)
    worker.start()

    deadline = time.monotonic() + args.timeout_seconds
    polls = 0
    failures: list[dict[str, object]] = []
    health_latencies: list[float] = []
    status_latencies: list[float] = []
    query_latencies: list[float] = []
    observed_migration = False
    promoted = False

    while time.monotonic() < deadline:
        polls += 1
        for name, method, url, body, latencies in (
            ("health", "GET", args.health_url, None, health_latencies),
            ("status", "GET", table_url, None, status_latencies),
            ("query", "POST", query_url, query_body, query_latencies),
        ):
            try:
                status, payload, elapsed = request_json(
                    method, url, body, args.request_timeout_seconds
                )
                latencies.append(elapsed)
                if status != 200:
                    failures.append({"poll": polls, "request": name, "status": status, "payload": payload})
                    continue
                if name == "status" and isinstance(payload, dict):
                    migration = payload.get("migration")
                    observed_migration = observed_migration or migration is not None
                    indexes = payload.get("indexes")
                    promoted = (
                        migration is None
                        and isinstance(indexes, dict)
                        and list(indexes) == [args.expected_index]
                    )
            except BaseException as error:
                failures.append({"poll": polls, "request": name, "error": repr(error)})

        if promoted and not worker.is_alive():
            break
        time.sleep(args.interval_seconds)

    worker.join(timeout=max(0.0, deadline - time.monotonic()))

    def latency_summary(samples: list[float]) -> dict[str, float | int]:
        return {
            "samples": len(samples),
            "max_ms": max(samples, default=0.0) * 1000.0,
            "mean_ms": (sum(samples) / len(samples) if samples else 0.0) * 1000.0,
        }

    result = {
        "schema_update": update,
        "polls": polls,
        "observed_migration": observed_migration,
        "promoted": promoted,
        "expected_index": args.expected_index,
        "failures": failures,
        "health": latency_summary(health_latencies),
        "status": latency_summary(status_latencies),
        "query": latency_summary(query_latencies),
    }
    print(json.dumps(result, sort_keys=True))

    return 0 if update.get("status") == 200 and promoted and not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
