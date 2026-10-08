#!/usr/bin/env python3
"""Verify a historical HN Parquet partition using a local Antfly and GCS.

Credentials are captured from gcloud into the child environment, never written
to configuration, results, or stdout. This does not change cloud IAM or buckets.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    parser.add_argument("--prefix", default="hn-poc/20261007")
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8877)
    parser.add_argument("--expected-rows", type=int, default=10000)
    parser.add_argument("--require-filters", action="store_true")
    args = parser.parse_args()
    args.state.mkdir(parents=True, exist_ok=True)
    token = subprocess.run(
        ["gcloud", "auth", "print-access-token", "--project", args.project],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    env = dict(os.environ, HN_GCS_BEARER=token)

    def connection(capability, prefix):
        return {
            "kind": "external_io",
            "capabilities": [capability],
            "external_io": {
                "protocol": "gcs",
                "buckets": [args.bucket],
                "prefix": prefix,
                "bucket_provisioning": "require_existing",
                "credentials": {
                    "source": "bearer_token",
                    "bearer_token": "${secret:HN_GCS_BEARER}",
                },
            },
        }

    config = {
        "storage": {
            "engine": "local",
            "local": {"base_dir": str(args.state / "data")},
            "artifacts": {
                "connection": "hn-gcs-artifacts",
                "bucket": args.bucket,
                "prefix": args.prefix + "/indexes",
            },
        },
        "connections": {
            "hn-gcs-source": connection("lake_read", args.prefix + "/archive"),
            "hn-gcs-artifacts": connection("storage.primary", args.prefix + "/indexes"),
        },
        "lake_cache": {"root": str(args.state / "cache"), "max_disk_bytes": 1073741824},
    }
    config_path = args.state / "config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    base = f"http://127.0.0.1:{args.port}/db/v1"
    process = None
    log = None

    def start():
        nonlocal process, log
        log = (args.state / "server.log").open("a")
        process = subprocess.Popen(
            [
                str(Path(args.binary).resolve()),
                "standalone",
                "--config",
                str(config_path),
                "--data-dir",
                str(args.state / "data"),
                "--host",
                "127.0.0.1",
                "--port",
                str(args.port),
                "--health",
                "false",
                "--auth",
                "false",
                "--models-dir",
                str(args.state / "empty-models"),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    f"Antfly exited with {process.returncode}; see {args.state / 'server.log'}"
                )
            try:
                call("GET", "/tables")
                return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.5)
        raise RuntimeError("Antfly startup timed out")

    def stop():
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if log is not None:
            log.close()

    def call(method, path, payload=None, timeout=90):
        request_started = time.monotonic()
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                value = json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            # Avoid echoing arbitrary server/provider bodies into task output.
            detail = exc.read().decode(errors="replace").replace(token, "[redacted]")
            raise RuntimeError(
                f"{method} {path}: HTTP {exc.code}: {detail[:1000]}"
            ) from exc
        print(
            json.dumps(
                {
                    "request": method + " " + path,
                    "elapsed_ms": round((time.monotonic() - request_started) * 1000, 2),
                }
            ),
            flush=True,
        )
        return (
            value["responses"][0]
            if isinstance(value, dict) and "responses" in value
            else value
        )

    def timed_query(query):
        started = time.monotonic()
        result = call("POST", "/tables/hn_archive_poc/query", query)
        return result, round((time.monotonic() - started) * 1000, 2)

    try:
        start()
        existing = call("GET", "/tables")
        if "hn_archive_poc" not in json.dumps(existing):
            call(
                "POST",
                "/tables/hn_archive_poc",
                {
                    "num_shards": 1,
                    "schema": {
                        "storage_mode": "relational",
                        "base_source": {
                            "kind": "external",
                            "table_id": "hn-archive-poc",
                            "format": "parquet",
                            "uri": f"gs://{args.bucket}/{args.prefix}/archive",
                            "credentials": {"ref": "hn-gcs-source"},
                        },
                    },
                    "indexes": {"body_text": {"type": "full_text", "field": "body"}},
                },
            )
        started = time.monotonic()
        deadline = started + 300
        while True:
            status = call("GET", "/tables/hn_archive_poc/indexes/body_text")
            if status.get("status", {}).get("readiness", {}).get("queryable"):
                break
            if status.get("status", {}).get("readiness", {}).get("state") == "failed":
                raise RuntimeError("Index construction failed: " + json.dumps(status))
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Index not queryable after 300 seconds: " + json.dumps(status)
                )
            time.sleep(1)
        ready_seconds = round(time.monotonic() - started, 2)
        count = call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn_archive_poc"}
        )
        assert count["rows"] == [[str(args.expected_rows)]], count
        query = {
            "full_text_search": {"match": "database", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["hn_id", "title", "item_type"],
            "highlight": {"fields": ["body"]},
            "limit": 5,
            "order_by": [{"field": "_score", "desc": True}],
        }
        first, first_ms = timed_query(query)
        if not first["hits"]["hits"]:
            query["full_text_search"]["match"] = "AI"
            first, first_ms = timed_query(query)
        hits = first["hits"]["hits"]
        assert len(hits) >= 2, "Sample must contain at least two matching rows"
        assert all(hit.get("_highlights", {}).get("body") for hit in hits), (
            "Missing remote highlights"
        )
        term = query["full_text_search"]["match"].lower()
        assert all(
            any(
                fragment["text"]
                .encode("utf-8")[span["start"] : span["end"]]
                .decode("utf-8")
                .lower()
                in (term, term + "s")
                for fragment in hit["_highlights"]["body"]
                for span in fragment["spans"]
            )
            for hit in hits
        ), "Highlight spans do not match the search term"
        assert all("body" not in hit["_source"] for hit in hits), (
            "Highlight widened source projection"
        )
        warm, warm_ms = timed_query(query)
        assert [h["_id"] for h in warm["hits"]["hits"]] == [h["_id"] for h in hits]
        first_page = call("POST", "/tables/hn_archive_poc/query", dict(query, limit=1))
        page_query = dict(
            query,
            limit=4,
            search_after=first_page["hits"]["hits"][0]["_sort"],
            remote_snapshot=first_page["remote_snapshot"],
        )
        second = call("POST", "/tables/hn_archive_poc/query", page_query)
        assert [h["_id"] for h in second["hits"]["hits"]] == [
            h["_id"] for h in hits[1:]
        ]
        selected = hits[0]["_source"]["hn_id"]
        filtered_query = dict(
            query, filter_query={"term": {"path": "/hn_id", "value": selected}}
        )
        filtered_sort_rejection = None
        exact_filter_status = "passed"
        try:
            filtered = call("POST", "/tables/hn_archive_poc/query", filtered_query)
        except RuntimeError as exc:
            if "filter_not_queryable" not in str(exc):
                raise
            filtered_sort_rejection = "filter_not_queryable"
            filtered_query.pop("order_by")
            try:
                filtered = call(
                    "POST", "/tables/hn_archive_poc/query", filtered_query, timeout=15
                )
            except TimeoutError:
                filtered = None
                exact_filter_status = "unordered_filter_timeout_15s"
        if filtered is not None:
            assert [h["_source"]["hn_id"] for h in filtered["hits"]["hits"]] == [
                selected
            ]
        if args.require_filters:
            assert (
                filtered_sort_rejection is None and exact_filter_status == "passed"
            ), (filtered_sort_rejection, exact_filter_status)
        stop()
        start()
        reopened, restart_ms = timed_query(query)
        assert [(h["_id"], h["_score"]) for h in reopened["hits"]["hits"]] == [
            (h["_id"], h["_score"]) for h in hits
        ]
        assert all(
            h.get("_highlights", {}).get("body") for h in reopened["hits"]["hits"]
        )
        continued = call("POST", "/tables/hn_archive_poc/query", page_query)
        assert [h["_id"] for h in continued["hits"]["hits"]] == [
            h["_id"] for h in hits[1:]
        ]
        report = {
            "project": args.project,
            "source": f"gs://{args.bucket}/{args.prefix}/archive",
            "artifacts": f"gs://{args.bucket}/{args.prefix}/indexes",
            "sql_count": count,
            "row_count": args.expected_rows,
            "ready_seconds": ready_seconds,
            "first_search_ms": first_ms,
            "warm_search_ms": warm_ms,
            "after_restart_ms": restart_ms,
            "query": query,
            "first_response": first,
            "filtered_score_sort_rejection": filtered_sort_rejection,
            "exact_filter_status": exact_filter_status,
            "verified": [
                "remote_bm25",
                "projected_highlights",
                "snapshot_pagination",
                "restart_scores",
                "restart_pagination",
            ],
            "note": "Temporary bucket has an eight-day deletion lifecycle. Timings are a 10k-row smoke check, not archive-scale benchmarks.",
        }
        if exact_filter_status == "passed":
            report["verified"].append("exact_filter")
        (args.state / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(
            json.dumps(
                {
                    k: v
                    for k, v in report.items()
                    if k not in ("first_response", "query", "sql_count")
                },
                indent=2,
            )
        )
    finally:
        stop()


if __name__ == "__main__":
    main()
