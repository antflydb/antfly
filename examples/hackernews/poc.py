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
import hashlib
import struct
import json
import shutil
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor
import os
import resource
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def wait_for_index(
    read_status,
    deadline,
    before_poll,
    observed,
    *,
    now=time.monotonic,
    sleep=time.sleep,
):
    """Allow fenced background recovery without extending the build deadline."""
    started = now()
    failures = observed.setdefault("transient_failures", {})
    while True:
        before_poll()
        status = read_status()
        observed["last_status"] = status
        detail = status.get("status", {})
        readiness = detail.get("readiness", {})
        if now() >= deadline:
            raise RuntimeError(
                "Index not queryable within the build deadline: " + json.dumps(status)
            )
        if readiness.get("queryable"):
            return status
        if readiness.get("state") == "failed":
            code = detail.get("error")
            if code not in {"Timeout", "DeadlineExceeded"}:
                raise RuntimeError("Index construction failed: " + json.dumps(status))
            elapsed = round(now() - started, 2)
            failure = failures.setdefault(
                code,
                {"error": code, "observations": 0, "first_elapsed_seconds": elapsed},
            )
            failure["observations"] += 1
            failure["last_elapsed_seconds"] = elapsed
        sleep(min(1, max(0, deadline - now())))


def prepare_filesystem_source(source, state):
    """Stage flat Parquet inputs in the filesystem object-store layout."""
    source = source.resolve()
    objects = source / "archive" / "buckets" / "antfly" / "objects"
    if objects.is_dir():
        return source
    files = sorted((source / "archive").glob("*.parquet"))
    if not files:
        raise ValueError(
            "Filesystem source needs archive/*.parquet or its native bucket layout"
        )
    root = state / "local-source"
    objects = root / "archive" / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    if any(
        path.name not in {item.name for item in files}
        for path in objects.glob("*.parquet")
    ):
        raise ValueError(
            "Staged source has extra Parquet objects; use a fresh qualification state"
        )
    for path in files:
        destination = objects / path.name
        with path.open("rb") as source_file:
            digest = hashlib.file_digest(source_file, "sha256").hexdigest()
        # FilesystemClient's AFOBJ001 envelope carries payload length, content-type
        # length and the payload SHA-256. Discovery/range offsets use that envelope.
        header = (
            b"AFOBJ001" + struct.pack("<QI", path.stat().st_size, 0) + digest.encode()
        )
        if destination.exists():
            with destination.open("rb") as staged:
                valid = (
                    staged.read(len(header)) == header
                    and hashlib.file_digest(staged, "sha256").hexdigest() == digest
                )
            if not valid:
                raise ValueError(
                    "Staged source changed; use a fresh qualification state"
                )
        else:
            temporary = destination.with_suffix(".staging")
            with temporary.open("wb") as output, path.open("rb") as input_file:
                output.write(header)
                shutil.copyfileobj(input_file, output, length=1024 * 1024)
            temporary.replace(destination)
    return root


def create_current_table(call, current):
    if current not in json.dumps(call("GET", "/tables")):
        call(
            "POST",
            "/tables/" + current,
            {
                "schema": {
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    **{
                                        name: {
                                            "type": "integer",
                                            "x-antfly-field": {
                                                "type": "numeric",
                                                "sortable": True,
                                            },
                                        }
                                        for name in ("hn_id", "created_at", "points")
                                    },
                                    **{
                                        name: {"type": "string"}
                                        for name in (
                                            "body",
                                            "title",
                                            "author",
                                            "item_type",
                                        )
                                    },
                                },
                            }
                        }
                    },
                },
                "indexes": {"body_text": {"type": "full_text"}},
            },
        )


def qualify_composed_sources(call, query, selected_source):
    """Exercise historical/current visibility and retain a mutable source cursor."""
    current, history = "hn_current_poc", "hn_archive_poc"
    create_current_table(call, current)
    selected, extra = selected_source["hn_id"], 2147483647
    assert selected != extra
    documents = {
        str(key): dict(
            selected_source,
            hn_id=key,
            body="antfly qualification replacement",
            title=title,
        )
        for key, title in (
            (selected, "HN qualification replacement"),
            (extra, "HN qualification current"),
        )
    }
    call(
        "POST",
        f"/tables/{current}/batch",
        {"inserts": documents, "sync_level": "full_index"},
    )
    selected_filter = {"term": {"path": "/hn_id", "value": selected}}
    union = dict(
        query,
        source={"union": [{"table": history}, {"table": current}]},
        source_ranking="rrf",
        filter_query=selected_filter,
    )
    historical = call("POST", "/query", union)["hits"]["hits"]
    assert len(historical) == 1 and historical[0]["_source"]["hn_id"] == selected
    overlay = {
        "overlay": {
            "base": {"table": history},
            "changes": {"table": current},
            "key": ["hn_id"],
        }
    }
    suppressed = call("POST", "/query", dict(union, source=overlay))
    assert not suppressed["hits"]["hits"], suppressed
    if "hackernews_poc" not in json.dumps(call("GET", "/sources")):
        call("POST", "/sources/hackernews_poc", {"source": overlay})
    composed = dict(
        query,
        source={"saved": "hackernews_poc"},
        source_ranking="rrf",
        full_text_search={"match": "antfly", "field": "body"},
        filter_query={
            "bool": {
                "should": [
                    {"term": {"path": "/hn_id", "value": key}}
                    for key in (selected, extra)
                ],
                "minimum_should_match": 1,
            }
        },
        limit=2,
    )
    all_hits = call("POST", "/query", composed)["hits"]["hits"]
    assert {hit["_source"]["hn_id"] for hit in all_hits} == {selected, extra}, all_hits
    first = call("POST", "/query", dict(composed, limit=1))
    continuation = dict(composed, source_cursor=first["next_source_cursor"])
    second = call("POST", "/query", continuation)["hits"]["hits"]
    assert len(second) == 1 and second[0]["_id"] != first["hits"]["hits"][0]["_id"]
    expected = [(hit["_id"], hit["_source"]["title"], hit["_score"]) for hit in second]
    for document in documents.values():
        document["title"] = "HN qualification updated after cursor capture"
    call(
        "POST",
        f"/tables/{current}/batch",
        {"inserts": documents, "sync_level": "full_index"},
    )
    fresh = call("POST", "/query", composed)["hits"]["hits"]
    assert len(fresh) == 2 and all(
        hit["_source"]["title"] == "HN qualification updated after cursor capture"
        for hit in fresh
    ), fresh
    return continuation, expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Run a Linux binary in a bounded local x86 container",
    )
    parser.add_argument("--docker-cpus", type=int, default=4)
    parser.add_argument("--docker-memory-gib", type=int, default=4)
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    parser.add_argument("--prefix", default="hn-poc/20261007")
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument(
        "--filesystem-source",
        type=Path,
        help="Use a local connection root containing archive/; no cloud credentials",
    )
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
    parser.add_argument("--workload-terms", nargs="+", default=[])
    parser.add_argument("--query-term", default="database")
    parser.add_argument(
        "--no-highlights",
        action="store_true",
        help="Match a BM25-only comparison workload",
    )
    parser.add_argument(
        "--profile", action="store_true", help="Record query execution diagnostics"
    )
    parser.add_argument("--cursor-pages", type=int, default=0)
    parser.add_argument("--qualify-composed-sources", action="store_true")
    parser.add_argument(
        "--benchmark-composed",
        action="store_true",
        help="Measure union and saved overlay with a recent-row native current fixture, independently after cache-cleared restarts",
    )
    parser.add_argument(
        "--disable-navigation-warming",
        action="store_true",
        help="Disable optional readiness prefetch to measure empty serving caches",
    )
    parser.add_argument(
        "--composed-current-rows",
        type=int,
        default=1000,
        help="Bounded recent-row sample copied into a native table for composition benchmarks",
    )
    parser.add_argument("--store-source", action="store_true")
    parser.add_argument(
        "--cursor-term", help="Separate broad query for deep cursor coverage"
    )
    parser.add_argument(
        "--bearer-stdin",
        action="store_true",
        help="Read a short-lived token from stdin instead of gcloud; never persist it",
    )
    parser.add_argument("--build-timeout", type=int, default=300)
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Publish and verify count, then stop before query measurements",
    )
    parser.add_argument("--cursor-retention-ms", type=int, default=300000)
    args = parser.parse_args()
    if not 2 <= args.composed_current_rows <= 10000:
        parser.error("composed current rows must be between 2 and 10000")
    if args.benchmark_composed and not args.qualify_composed_sources:
        parser.error("composed benchmarks require --qualify-composed-sources")
    if args.docker and not args.filesystem_source:
        parser.error("Docker comparison mode requires --filesystem-source")
    if args.docker_cpus < 1 or args.docker_memory_gib < 1:
        parser.error("Docker limits must be positive")
    args.state = args.state.resolve()
    if not 1 <= args.port < 65535:
        parser.error("query port must leave a valid adjacent health port")
    if not 0 <= args.cursor_pages <= 100:
        parser.error("cursor pages must be between zero and 100")
    if args.repeats < 1 or args.concurrency < 1:
        parser.error("repeats and concurrency must be positive")
    if args.expected_rows < 2 or args.build_timeout < 1:
        parser.error("expected rows must be >=2 and build timeout must be positive")
    if not 1000 <= args.cursor_retention_ms <= 3600000:
        parser.error("cursor retention must be between 1000 and 3600000 milliseconds")
    artifact_prefix = args.artifact_prefix or args.prefix
    metadata_columns = (
        []
        if args.text_only
        else ["hn_id", "created_at", "item_type", "author", "points"]
    )
    args.state.mkdir(parents=True, exist_ok=True)
    if args.filesystem_source:
        token = ""
    elif args.bearer_stdin:
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
    local_source_root = (
        prepare_filesystem_source(args.filesystem_source, args.state)
        if args.filesystem_source
        else None
    )
    env = dict(os.environ, HN_GCS_BEARER=token)

    def connection(capability, prefix):
        if args.filesystem_source:
            root = (
                local_source_root
                if capability == "lake_read"
                else args.state.resolve() / "artifacts"
            )
            return {
                "kind": "external_io",
                "capabilities": [capability],
                "external_io": {"protocol": "filesystem", "root": str(root)},
            }
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
        "lake_indexes": {
            "query_cursors": {"retention_ms": args.cursor_retention_ms},
            "navigation_warming": {"enabled": not args.disable_navigation_warming},
        },
    }
    if args.filesystem_source:
        # The native local store publishes through its filesystem artifact store.
        # The source connection still confines reads to the supplied root.
        del config["storage"]["artifacts"]
    config_path = args.state / "config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    base = f"http://127.0.0.1:{args.port}/db/v1"
    process = None
    log = None
    container = None

    startup_samples_ms = []

    def start():
        startup_started = time.monotonic()
        nonlocal process, log, container
        log = (args.state / "server.log").open("a")
        command = [
            str(Path(args.binary).resolve()),
            "standalone",
            "--config",
            str(config_path),
            "--data-dir",
            str(args.state / "data"),
            "--host",
            "0.0.0.0" if args.docker else "127.0.0.1",
            "--port",
            str(args.port),
            "--health",
            "true",
            "--health-port",
            str(args.port + 1),
            "--auth",
            "false",
            "--models-dir",
            str(args.state / "empty-models"),
        ]
        if args.docker:
            container = "hn-comparison-" + uuid.uuid4().hex
            command[0] = "/app/antfly"
            command = [
                "docker",
                "run",
                "--rm",
                "--name",
                container,
                "--platform",
                "linux/amd64",
                "--cpus",
                str(args.docker_cpus),
                "--memory",
                f"{args.docker_memory_gib}g",
                "-p",
                f"127.0.0.1:{args.port}:{args.port}",
                "-p",
                f"127.0.0.1:{args.port + 1}:{args.port + 1}",
                "--mount",
                f"type=bind,src={Path(args.binary).resolve()},dst=/app/antfly,readonly",
                "--mount",
                f"type=bind,src={args.state},dst={args.state}",
                "--mount",
                f"type=bind,src={args.filesystem_source.resolve()},dst={args.filesystem_source.resolve()},readonly",
                "alpine:3.22",
                *command,
            ]
        process = subprocess.Popen(
            command,
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
                startup_samples_ms.append(
                    round((time.monotonic() - startup_started) * 1000, 2)
                )
                return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(0.5)
        raise RuntimeError("Antfly startup timed out")

    def stop():
        nonlocal container
        if container is not None:
            subprocess.run(
                ["docker", "rm", "-f", container],
                capture_output=True,
                timeout=30,
                check=False,
            )
            container = None
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
            detail = exc.read().decode(errors="replace")
            if token:
                detail = detail.replace(token, "[redacted]")
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
        if args.docker or not Path("/proc/net/dev").exists():
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

    def lake_metrics():
        # Health exposition refreshes every five seconds. Sample serial phases
        # after a full refresh interval, outside their measured query latency.
        time.sleep(6)
        with urllib.request.urlopen(
            f"http://127.0.0.1:{args.port + 1}/metrics", timeout=15
        ) as response:
            text = response.read().decode()
        return {
            fields[0]: int(fields[1])
            for line in text.splitlines()
            if line.startswith("antfly_lake_")
            and len(fields := line.split()) == 2
            and "{" not in fields[0]
        }

    def timed_query(query, phase="search", path="/tables/hn_archive_poc/query"):
        serial_profile = phase in {
            "cold",
            "restart",
            "cursor_initial",
            "cursor_depth_restart",
        } or phase.startswith("composed_cold.")
        metrics_before = lake_metrics() if serial_profile else None
        before = resource_counters()
        started = time.monotonic()
        result = call("POST", path, query)
        elapsed = round((time.monotonic() - started) * 1000, 2)
        after = resource_counters()
        metrics_after = lake_metrics() if serial_profile else None
        if before is not None or metrics_after is not None:
            io_profiles.append(
                {
                    "phase": phase,
                    "elapsed_ms": elapsed,
                    "took_ms": result.get("took"),
                    "filter": query.get("filter_query"),
                    "lake_metrics_delta": (
                        {
                            key: value - metrics_before.get(key, 0)
                            for key, value in metrics_after.items()
                            if key.endswith("_total")
                        }
                        if metrics_after is not None
                        else None
                    ),
                    "lake_metrics_scope": "process; nested phases may overlap",
                    "lake_metrics_before": metrics_before,
                    **{key: round(after[key] - before[key], 2) for key in before or {}},
                }
            )
        return result, elapsed

    phase = "startup"
    try:
        start()
        existing = call("GET", "/tables")
        registration_seconds = 0
        if "hn_archive_poc" not in json.dumps(existing):
            registration_started = time.monotonic()
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
                            "uri": "file:///archive"
                            if args.filesystem_source
                            else f"gs://{args.bucket}/{args.prefix}/archive",
                            "credentials": {"ref": "hn-gcs-source"},
                        },
                    },
                    "indexes": {
                        "body_text": {
                            "type": "full_text",
                            "field": "body",
                            "store_source": args.store_source,
                        }
                    },
                },
            )
            registration_seconds = round(time.monotonic() - registration_started, 2)
        phase = "index_build"
        started = time.monotonic()
        build_before = resource_counters()
        peak_rss_bytes = 0
        deadline = started + args.build_timeout

        def sample_build_memory():
            nonlocal peak_rss_bytes
            if Path(f"/proc/{process.pid}/status").exists():
                for line in (
                    Path(f"/proc/{process.pid}/status").read_text().splitlines()
                ):
                    if line.startswith("VmHWM:"):
                        peak_rss_bytes = max(
                            peak_rss_bytes, int(line.split()[1]) * 1024
                        )

        build_observed = {}
        status = wait_for_index(
            lambda: call("GET", "/tables/hn_archive_poc/indexes/body_text"),
            deadline,
            sample_build_memory,
            build_observed,
        )
        ready_seconds = round(time.monotonic() - started, 2)
        build_after = resource_counters()
        build_profile = (
            None
            if build_before is None
            else {key: build_after[key] - value for key, value in build_before.items()}
        )
        metadata_status = {}
        for column in metadata_columns:
            # Relational indexes are published with the pinned lake snapshot.
            metadata_status[column] = call(
                "GET", "/tables/hn_archive_poc/indexes/" + column + "_idx"
            )
            assert metadata_status[column]["status"]["readiness"]["queryable"], (
                metadata_status[column]
            )
        phase = "row_count"
        count = call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn_archive_poc"}
        )
        assert count["rows"] == [[str(args.expected_rows)]], count
        if args.build_only:
            (args.state / "construction.json").write_text(
                json.dumps(
                    {
                        "rows": args.expected_rows,
                        "ready_seconds": ready_seconds,
                        "registration_seconds": registration_seconds,
                        "metadata_indexes_queryable": list(metadata_status),
                        "query_measurements_performed": False,
                    },
                    indent=2,
                )
                + "\n"
            )
            return
        if args.cold_cache:
            stop()
            if (args.state / "cache").exists():
                shutil.rmtree(args.state / "cache")
            start()
        if not args.disable_navigation_warming:
            readiness = call("GET", "/tables/hn_archive_poc/indexes/body_text")
            assert readiness["status"]["readiness"]["queryable"], readiness
        if args.disable_navigation_warming:
            baseline_metrics = lake_metrics()
            assert (
                baseline_metrics.get("antfly_lake_navigation_warming_attempts_total", 0)
                == 0
            ), "Warming was unexpectedly active in the empty-cache scenario"
        cache_files_before_search = sum(
            p.is_file() for p in (args.state / "cache").rglob("*")
        )
        phase = "query_correctness_and_latency"
        query = {
            "full_text_search": {"match": args.query_term, "field": "body"},
            "full_text_index": "body_text",
            "fields": ["hn_id", "title", "item_type", "author", "points", "created_at"],
            "highlight": {"fields": ["body"]},
            "limit": 5,
            "order_by": [{"field": "_score", "desc": True}],
        }
        if args.no_highlights:
            del query["highlight"]
        if args.profile:
            query["profile"] = True
        first, first_ms = timed_query(query, "cold")
        if not first["hits"]["hits"] and not args.no_highlights:
            query["full_text_search"]["match"] = "AI"
            first, first_ms = timed_query(query, "cold")
        hits = first["hits"]["hits"]
        assert len(hits) >= 2, "Sample must contain at least two matching rows"
        term = query["full_text_search"]["match"].lower()
        if not args.no_highlights:
            assert all(hit.get("_highlights", {}).get("body") for hit in hits), (
                "Missing remote highlights"
            )
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
        workload_results = {}
        for workload_term in args.workload_terms:
            workload_query = dict(
                query, full_text_search={"match": workload_term, "field": "body"}
            )
            samples = []
            expected = None
            for _ in range(args.repeats):
                result, elapsed = timed_query(
                    workload_query, "workload." + workload_term
                )
                signature = [(h["_id"], h["_score"]) for h in result["hits"]["hits"]]
                if expected is not None:
                    assert signature == expected, (
                        "Workload results changed: " + workload_term
                    )
                expected = signature
                samples.append(elapsed)
            workload_results[workload_term] = {
                "samples_ms": samples,
                "total": result["hits"]["total"],
                "top_hits": expected,
            }
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
        cursor_query = query
        deep_snapshot = first_page["remote_snapshot"]
        cursor_depth = {
            "hits": 1 + len(second["hits"]["hits"]),
            "samples_ms": [],
            "term": term,
        }
        deep_page = second
        seen = {h["_id"] for h in first_page["hits"]["hits"] + second["hits"]["hits"]}
        if args.cursor_term:
            cursor_query = dict(
                query, full_text_search={"match": args.cursor_term, "field": "body"}
            )
            deep_page, initial_ms = timed_query(
                dict(cursor_query, limit=128), "cursor_initial"
            )
            deep_snapshot = deep_page["remote_snapshot"]
            initial_ids = [h["_id"] for h in deep_page["hits"]["hits"]]
            assert len(set(initial_ids)) == len(initial_ids), (
                "Cursor repeated initial hits"
            )
            seen = set(initial_ids)
            cursor_depth = {
                "hits": len(seen),
                "samples_ms": [],
                "term": args.cursor_term,
                "initial_ms": initial_ms,
            }
        deep_request = None
        deep_signature = None
        for _ in range(args.cursor_pages):
            if not deep_page["hits"]["hits"]:
                break
            candidate_request = dict(
                cursor_query,
                limit=128,
                search_after=deep_page["hits"]["hits"][-1]["_sort"],
                remote_snapshot=deep_snapshot,
            )
            deep_page, elapsed = timed_query(candidate_request, "cursor_depth")
            cursor_depth["samples_ms"].append(elapsed)
            if not deep_page["hits"]["hits"]:
                break
            deep_request = candidate_request
            deep_signature = [
                (h["_id"], h["_score"]) for h in deep_page["hits"]["hits"]
            ]
            ids = [h["_id"] for h in deep_page["hits"]["hits"]]
            assert len(set(ids)) == len(ids) and not seen.intersection(ids), (
                "Cursor repeated hits"
            )
            seen.update(ids)
            cursor_depth["hits"] += len(ids)
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
        composed_cursor = None
        composed_verified = []
        composed_error = None
        if args.qualify_composed_sources:
            phase = "composed_sources"
            try:
                composed_cursor, composed_expected = qualify_composed_sources(
                    call, query, hits[0]["_source"]
                )
            except (RuntimeError, AssertionError) as exc:
                # Preserve baseline restart measurements while the required
                # composed-source gate still fails. Never save provider bodies.
                composed_error = type(exc).__name__
        stop()
        start()
        reopened, restart_ms = timed_query(query, "restart")
        if composed_cursor is not None:
            retained = call("POST", "/query", composed_cursor)["hits"]["hits"]
            assert [
                (hit["_id"], hit["_source"]["title"], hit["_score"]) for hit in retained
            ] == composed_expected, retained
            composed_verified = [
                "union_history",
                "overlay_nonmatching_suppression",
                "saved_current_visibility",
                "mutable_composed_cursor_restart",
            ]
        assert [(h["_id"], h["_score"]) for h in reopened["hits"]["hits"]] == [
            (h["_id"], h["_score"]) for h in hits
        ]
        if not args.no_highlights:
            assert all(
                h.get("_highlights", {}).get("body") for h in reopened["hits"]["hits"]
            )
        if deep_request is not None:
            deep_reopened, elapsed = timed_query(deep_request, "cursor_depth_restart")
            assert [
                (h["_id"], h["_score"]) for h in deep_reopened["hits"]["hits"]
            ] == deep_signature
            cursor_depth["restart_ms"] = elapsed
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
        composed_benchmarks = {}
        if args.benchmark_composed and composed_cursor is not None:
            current_benchmark = "hn_current_benchmark"
            create_current_table(call, current_benchmark)
            columns = [
                "hn_id",
                "body",
                "title",
                "item_type",
                "author",
                "points",
                "created_at",
            ]
            if args.filesystem_source:
                import pyarrow.dataset as dataset

                paths = sorted((args.filesystem_source / "archive").glob("*.parquet"))
                if not paths:
                    raise ValueError(
                        "Local composed fixture needs original archive/*.parquet payloads"
                    )
                recent_rows = (
                    dataset.dataset([str(path) for path in paths], format="parquet")
                    .to_table(columns=columns)
                    .sort_by([("created_at", "descending"), ("hn_id", "ascending")])
                    .slice(0, args.composed_current_rows)
                    .to_pylist()
                )
                recent = {
                    "rows": [[row[name] for name in columns] for row in recent_rows]
                }
            else:
                recent = call(
                    "POST",
                    "/sql",
                    {
                        "statement": "SELECT "
                        + ", ".join(columns)
                        + " FROM hn_archive_poc ORDER BY created_at DESC LIMIT "
                        + str(args.composed_current_rows)
                    },
                )
            assert len(recent["rows"]) == args.composed_current_rows
            documents = []
            for row in recent["rows"]:
                document = {
                    name: value
                    for name, value in zip(columns, row, strict=True)
                    if value is not None
                }
                for column in ("hn_id", "points", "created_at"):
                    if column in document:
                        document[column] = int(document[column])
                documents.append(document)
            inserts = {}
            batch_bytes = 0
            for document in documents:
                document_bytes = len(json.dumps(document).encode()) + 32
                if inserts and (
                    len(inserts) >= 128 or batch_bytes + document_bytes > 512 * 1024
                ):
                    call(
                        "POST",
                        f"/tables/{current_benchmark}/batch",
                        {"inserts": inserts, "sync_level": "full_index"},
                    )
                    inserts, batch_bytes = {}, 0
                inserts[str(document["hn_id"])] = document
                batch_bytes += document_bytes
            if inserts:
                call(
                    "POST",
                    f"/tables/{current_benchmark}/batch",
                    {"inserts": inserts, "sync_level": "full_index"},
                )
            benchmark_count = call(
                "POST",
                "/sql",
                {"statement": "SELECT COUNT(*) FROM " + current_benchmark},
            )
            assert benchmark_count["rows"] == [[str(args.composed_current_rows)]], (
                benchmark_count
            )
            benchmark_overlay = {
                "overlay": {
                    "base": {"table": "hn_archive_poc"},
                    "changes": {"table": current_benchmark},
                    "key": ["hn_id"],
                }
            }
            if "hackernews_benchmark" not in json.dumps(call("GET", "/sources")):
                call(
                    "POST",
                    "/sources/hackernews_benchmark",
                    {"source": benchmark_overlay},
                )
            for name, source in (
                (
                    "union",
                    {
                        "union": [
                            {"table": "hn_archive_poc"},
                            {"table": current_benchmark},
                        ]
                    },
                ),
                ("saved_overlay", {"saved": "hackernews_benchmark"}),
            ):
                stop()
                if (args.state / "cache").exists():
                    shutil.rmtree(args.state / "cache")
                start()
                request = dict(query, source=source, source_ranking="rrf")
                cold, cold_ms = timed_query(request, "composed_cold." + name, "/query")
                expected = [(h["_id"], h["_score"]) for h in cold["hits"]["hits"]]
                samples = []
                for _ in range(args.repeats):
                    result, elapsed = timed_query(
                        request, "composed_warm." + name, "/query"
                    )
                    assert [
                        (h["_id"], h["_score"]) for h in result["hits"]["hits"]
                    ] == expected
                    samples.append(elapsed)
                composed_benchmarks[name] = {
                    "first_search_ms": cold_ms,
                    "warm_samples_ms": samples,
                    "warm_p95_ms": sorted(samples)[
                        min(len(samples) - 1, int(len(samples) * 0.95))
                    ],
                    "result_count": len(expected),
                    "current_fixture_rows": args.composed_current_rows,
                    "note": "Full sample history plus its most recent rows copied into a native current table; bounded diagnostic fixture, not production current-table scale.",
                }
        stop()
        engine_peak_rss_bytes = (
            None
            if args.docker
            else resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
            * (1 if sys.platform == "darwin" else 1024)
        )
        cache_files = [p for p in (args.state / "cache").rglob("*") if p.is_file()]
        report = {
            "project": None if args.filesystem_source else args.project,
            "metadata_indexes": metadata_columns,
            "metadata_indexes_queryable": list(metadata_status),
            "cold_cache_cleared_after_publication": args.cold_cache,
            "navigation_warming_enabled": not args.disable_navigation_warming,
            "startup_samples_ms": startup_samples_ms,
            "engine_child_peak_rss_bytes": engine_peak_rss_bytes,
            "engine_child_peak_rss_scope": "Maximum across child processes over the entire run, including publication if performed; not query-only RSS.",
            "composed_benchmarks": composed_benchmarks,
            "cache_files_before_search": cache_files_before_search,
            "warm_samples_ms": warm_samples,
            "concurrent_search_ms": [elapsed for _, elapsed in concurrent_samples],
            "concurrency": args.concurrency,
            "cursor_retention_ms": args.cursor_retention_ms,
            "metadata_filters": filter_results,
            "workloads": workload_results,
            "cursor_depth": cursor_depth,
            "composed_sources": {
                "verified": composed_verified,
                "error": composed_error,
            },
            "io_profiles": io_profiles,
            "io_profile_note": "Linux process CPU and pod-wide non-loopback network counters. Concurrent intervals overlap; not per-query byte attribution. Includes TLS/DNS/control metadata; not a GCS request trace.",
            "cache_files": len(cache_files),
            "cache_bytes": sum(p.stat().st_size for p in cache_files),
            "store_source": args.store_source,
            "source": (args.filesystem_source.resolve() / "archive").as_uri()
            if args.filesystem_source
            else f"gs://{args.bucket}/{args.prefix}/archive",
            "artifacts": "native filesystem artifact store"
            if args.filesystem_source
            else f"gs://{args.bucket}/{artifact_prefix}/indexes",
            "sql_count": count,
            "row_count": args.expected_rows,
            "ready_seconds": ready_seconds,
            "transient_build_failures": list(
                build_observed["transient_failures"].values()
            ),
            "registration_seconds": registration_seconds,
            "build_profile": build_profile,
            "build_peak_rss_bytes": peak_rss_bytes,
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
            "note": "Local filesystem comparison; OS file caches are not flushed."
            if args.filesystem_source
            else "Qualification of the reported row count. The temporary demo bucket has an eight-day deletion lifecycle.",
        }
        if args.no_highlights:
            report["verified"].remove("projected_highlights")
        if args.docker:
            report["execution_environment"] = {
                "architecture": "x86_64",
                "cpus": args.docker_cpus,
                "memory_gib": args.docker_memory_gib,
                "runtime": "Docker; host translation may apply",
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
    except Exception as exc:
        # Do not persist arbitrary provider error bodies or credentials.
        failure = {"phase": phase, "exception": type(exc).__name__}
        if "started" in locals():
            failure["elapsed_since_build_start_seconds"] = round(
                time.monotonic() - started, 2
            )
        if "registration_seconds" in locals():
            failure["registration_seconds"] = registration_seconds
        if "peak_rss_bytes" in locals():
            failure["build_peak_rss_bytes"] = peak_rss_bytes
        # Retain completed timings when a later workload fails. They are
        # partial observations, never a passing correctness/qualification report.
        scope = locals()
        measurements = {"complete": False}
        for name, target in (
            ("first_ms", "first_search_ms"),
            ("warm_samples", "warm_samples_ms"),
            ("workload_results", "workloads"),
            ("cursor_depth", "cursor_depth"),
            ("restart_ms", "after_restart_ms"),
            ("io_profiles", "io_profiles"),
            ("ready_seconds", "ready_seconds"),
            ("build_profile", "build_profile"),
            ("filter_results", "metadata_filters"),
            ("count", "sql_count"),
        ):
            if name in scope:
                measurements[target] = scope[name]
        if len(measurements) > 1:
            failure["completed_measurements"] = measurements
        if "build_observed" in locals():
            failure["transient_build_failures"] = list(
                build_observed["transient_failures"].values()
            )
            if phase == "index_build" and "last_status" in build_observed:
                failure["index_status"] = build_observed["last_status"]
        (args.state / "failure.json").write_text(json.dumps(failure, indent=2) + "\n")
        raise
    finally:
        stop()


if __name__ == "__main__":
    main()
