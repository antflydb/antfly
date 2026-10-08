#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify a historical HN Parquet partition using a local Antfly and GCS.

Credentials are captured from gcloud into the child environment, never written
to configuration, results, or stdout. This does not change cloud IAM or buckets.
"""

import argparse
import json
import shutil
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor
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
    parser.add_argument(
        "--artifact-prefix", help="Separate namespace for index artifacts"
    )
    parser.add_argument(
        "--text-only",
        action="store_true",
        help="Benchmark projected scans without metadata indexes",
    )
    parser.add_argument(
        "--cold-cache",
        action="store_true",
        help="Stop after publication, remove this state's cache, then measure",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--bearer-stdin",
        action="store_true",
        help="Read a short-lived token from stdin instead of gcloud; never persist it",
    )
    args = parser.parse_args()
    if args.repeats < 1 or args.concurrency < 1:
        parser.error("repeats and concurrency must be positive")
    artifact_prefix = args.artifact_prefix or args.prefix
    metadata_columns = (
        []
        if args.text_only
        else ["hn_id", "created_at", "item_type", "author", "points"]
    )
    args.state.mkdir(parents=True, exist_ok=True)
    if args.bearer_stdin:
        token = sys.stdin.readline(65537).strip()
        if not token or len(token) > 65536:
            raise ValueError("Expected a bounded bearer token on stdin")
    else:
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
                "prefix": artifact_prefix + "/indexes",
            },
        },
        "connections": {
            "hn-gcs-source": connection("lake_read", args.prefix + "/archive"),
            "hn-gcs-artifacts": connection(
                "storage.primary", artifact_prefix + "/indexes"
            ),
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

    io_profiles = []

    def resource_counters():
        # Linux counters help separate CPU work from remote transfer. Network
        # counters cover non-loopback interfaces, including TLS/DNS overhead.
        if not Path("/proc/net/dev").exists():
            return None
        counters = {"network_rx_bytes": 0, "network_tx_bytes": 0}
        for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
            name, values = line.split(":", 1)
            if name.strip() != "lo":
                fields = values.split()
                counters["network_rx_bytes"] += int(fields[0])
                counters["network_tx_bytes"] += int(fields[8])
        fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
        counters["cpu_ms"] = (
            (int(fields[11]) + int(fields[12])) * 1000 / os.sysconf("SC_CLK_TCK")
        )
        return counters

    def timed_query(query, phase="search"):
        before = resource_counters()
        started = time.monotonic()
        result = call("POST", "/tables/hn_archive_poc/query", query)
        elapsed = round((time.monotonic() - started) * 1000, 2)
        after = resource_counters()
        if before is not None:
            io_profiles.append(
                {
                    "phase": phase,
                    "elapsed_ms": elapsed,
                    "took_ms": result.get("took"),
                    "filter": query.get("filter_query"),
                    **{key: round(after[key] - before[key], 2) for key in before},
                }
            )
        return result, elapsed

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
                        "relational_indexes": [
                            {"name": column + "_idx", "keys": [{"column": column}]}
                            for column in metadata_columns
                        ],
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
        metadata_status = {}
        for column in metadata_columns:
            # Relational indexes are published with the pinned lake snapshot.
            metadata_status[column] = call(
                "GET", "/tables/hn_archive_poc/indexes/" + column + "_idx"
            )
            assert metadata_status[column]["status"]["readiness"]["queryable"], (
                metadata_status[column]
            )
        count = call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn_archive_poc"}
        )
        assert count["rows"] == [[str(args.expected_rows)]], count
        if args.cold_cache:
            stop()
            if (args.state / "cache").exists():
                shutil.rmtree(args.state / "cache")
            start()
        cache_files_before_search = sum(
            p.is_file() for p in (args.state / "cache").rglob("*")
        )
        query = {
            "full_text_search": {"match": "database", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["hn_id", "title", "item_type", "author", "points", "created_at"],
            "highlight": {"fields": ["body"]},
            "limit": 5,
            "order_by": [{"field": "_score", "desc": True}],
        }
        first, first_ms = timed_query(query, "cold")
        if not first["hits"]["hits"]:
            query["full_text_search"]["match"] = "AI"
            first, first_ms = timed_query(query, "cold")
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
        warm, warm_ms = timed_query(query, "warm")
        assert [h["_id"] for h in warm["hits"]["hits"]] == [h["_id"] for h in hits]
        warm_samples = [warm_ms]
        for _ in range(args.repeats - 1):
            repeated, elapsed = timed_query(query, "warm")
            assert [h["_id"] for h in repeated["hits"]["hits"]] == [
                h["_id"] for h in hits
            ]
            warm_samples.append(elapsed)
        warm_ms = round(statistics.median(warm_samples), 2)
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            concurrent_samples = list(
                pool.map(
                    lambda _: timed_query(query, "concurrent"), range(args.concurrency)
                )
            )
        for result, _ in concurrent_samples:
            assert [h["_id"] for h in result["hits"]["hits"]] == [
                h["_id"] for h in hits
            ]
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
        unordered_filter_ms = None
        if filtered_sort_rejection is None:
            unordered_query = dict(filtered_query)
            unordered_query.pop("order_by", None)
            unordered_started = time.monotonic()
            unordered = call(
                "POST", "/tables/hn_archive_poc/query", unordered_query, timeout=15
            )
            unordered_filter_ms = round(
                (time.monotonic() - unordered_started) * 1000, 2
            )
            assert [h["_source"]["hn_id"] for h in unordered["hits"]["hits"]] == [
                selected
            ]
        source = hits[0]["_source"]
        predicates = {
            "item_type": {"term": {"path": "/item_type", "value": source["item_type"]}},
            "author": {"term": {"path": "/author", "value": source["author"]}},
            "points": {"range": {"path": "/points", "gte": source["points"]}},
            "created_at": {
                "range": {
                    "path": "/created_at",
                    "gte": source["created_at"],
                    "lt": source["created_at"] + 3600,
                }
            },
        }
        predicates["combined"] = {"bool": {"filter": list(predicates.values())}}

        def check_predicate(name, row):
            checks = {
                "item_type": row["item_type"] == source["item_type"],
                "author": row["author"] == source["author"],
                "points": row["points"] >= source["points"],
                "created_at": source["created_at"]
                <= row["created_at"]
                < source["created_at"] + 3600,
            }
            return all(checks.values()) if name == "combined" else checks[name]

        filter_results = {}
        for name, predicate in predicates.items():
            samples = []
            filter_request = dict(query, filter_query=predicate)
            for _ in range(args.repeats):
                result, elapsed = timed_query(filter_request, "filter." + name)
                rows = [h["_source"] for h in result["hits"]["hits"]]
                assert rows and all(check_predicate(name, row) for row in rows), (
                    name,
                    rows,
                )
                assert selected in [row["hn_id"] for row in rows], (name, rows)
                samples.append(elapsed)
            # Force authoritative residual evaluation as an independent correctness oracle.
            residual_request = dict(
                filter_request,
                filter_query={
                    "bool": {
                        "should": [predicate, {"match_all": {}}],
                        "minimum_should_match": 2,
                    }
                },
            )
            reference = call("POST", "/tables/hn_archive_poc/query", residual_request)
            assert [(h["_id"], h["_score"]) for h in reference["hits"]["hits"]] == [
                (h["_id"], h["_score"]) for h in result["hits"]["hits"]
            ], name
            assert reference["hits"]["total"] == result["hits"]["total"], name
            filter_results[name] = {
                "total": result["hits"]["total"],
                "samples_ms": samples,
                "median_ms": round(statistics.median(samples), 2),
                "predicate": predicate,
            }
        no_match = dict(
            query, filter_query={"range": {"path": "/created_at", "gte": 4102444800}}
        )
        empty, _ = timed_query(no_match, "empty_filter")
        assert not empty["hits"]["hits"], "Out-of-sample date range returned hits"
        stop()
        start()
        reopened, restart_ms = timed_query(query, "restart")
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
        for name, predicate in predicates.items():
            result, elapsed = timed_query(
                dict(query, filter_query=predicate), "restart_filter." + name
            )
            assert all(
                check_predicate(name, h["_source"]) for h in result["hits"]["hits"]
            ), name
            assert selected in [
                h["_source"]["hn_id"] for h in result["hits"]["hits"]
            ], name
            assert result["hits"]["total"] == filter_results[name]["total"], name
            filter_results[name]["after_restart_ms"] = elapsed
        empty, _ = timed_query(no_match, "restart_empty_filter")
        assert not empty["hits"]["hits"], (
            "Out-of-sample date range returned hits after restart"
        )
        cache_files = [p for p in (args.state / "cache").rglob("*") if p.is_file()]
        report = {
            "project": args.project,
            "metadata_indexes": metadata_columns,
            "metadata_indexes_queryable": list(metadata_status),
            "cold_cache_cleared_after_publication": args.cold_cache,
            "cache_files_before_search": cache_files_before_search,
            "warm_samples_ms": warm_samples,
            "concurrent_search_ms": [elapsed for _, elapsed in concurrent_samples],
            "concurrency": args.concurrency,
            "metadata_filters": filter_results,
            "io_profiles": io_profiles,
            "io_profile_note": "Linux process CPU and pod-wide non-loopback network counters. Concurrent intervals overlap; not per-query byte attribution. Includes TLS/DNS/control metadata; not a GCS request trace.",
            "cache_files": len(cache_files),
            "cache_bytes": sum(p.stat().st_size for p in cache_files),
            "source": f"gs://{args.bucket}/{args.prefix}/archive",
            "artifacts": f"gs://{args.bucket}/{artifact_prefix}/indexes",
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
            "unordered_filter_ms": unordered_filter_ms,
            "verified": [
                "remote_bm25",
                "projected_highlights",
                "snapshot_pagination",
                "restart_scores",
                "restart_pagination",
                "metadata_filters_match_residual",
                "metadata_filters_after_restart",
                "empty_date_filter_before_and_after_restart",
                "concurrent_search",
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
