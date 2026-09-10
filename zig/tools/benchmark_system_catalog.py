#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.
"""Benchmark system catalog workflows against disposable real Antfly servers.

Run from zig/: uv run --project e2e/antfly python tools/benchmark_system_catalog.py
The catalog scenario supports standalone or a three-data-node Raft cluster.
Resolution always uses the cluster, including cross-shard candidate reads,
atomic promotion, and graph hydration. Setup, warmup, and measured work are
reported separately; results are observations, never timing assertions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import requests

ZIG_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ZIG_ROOT / "e2e" / "antfly"))
from conftest import StandaloneAntflyServer, antfly_public_api_url
from test_resolution import DOCUMENTS_INDEXES
from test_scaling import MultiNodeScalingCluster


def positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def summary(durations: list[float]) -> dict:
    values = sorted(durations)
    return {
        "samples": len(values),
        "p50_ms": statistics.median(values),
        "p95_ms": values[math.ceil(len(values) * 0.95) - 1],
        "max_ms": max(values),
    }


class Api:
    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self.session = requests.Session()

    def request(self, method: str, path: str, body=None, *, ndjson=False):
        options = (
            {"data": body, "headers": {"Content-Type": "application/x-ndjson"}}
            if ndjson
            else {"json": body}
        )
        response = self.session.request(method, self.base + path, timeout=30, **options)
        if not response.ok:
            raise RuntimeError(
                f"{method} {path}: {response.status_code} {response.text[:1000]}"
            )
        if not response.content:
            return None
        if ndjson:
            value = [json.loads(line) for line in response.text.splitlines() if line]
        else:
            value = response.json()
        # Some query failures are carried inside a successful HTTP envelope.
        for item in value if isinstance(value, list) else [value]:
            if not isinstance(item, dict):
                continue
            for result in item.get("responses", []):
                if result.get("status", 200) >= 400:
                    raise RuntimeError(f"query failed: {result}")
        return value

    def measure(self, operation, samples: int, warmup: int) -> dict:
        for _ in range(warmup):
            operation()
        elapsed = []
        for _ in range(samples):
            start = time.perf_counter_ns()
            operation()
            elapsed.append((time.perf_counter_ns() - start) / 1e6)
        return summary(elapsed)


def concurrent_lookups(base: str, path: str, args) -> dict:
    barrier = threading.Barrier(args.concurrency, timeout=30)

    def worker(_):
        api = Api(base)
        try:
            for _ in range(args.warmup):
                api.request("GET", path)
            barrier.wait()
            durations = []
            started = time.perf_counter_ns()
            for _ in range(args.samples):
                start = time.perf_counter_ns()
                value = api.request("GET", path)
                if value.get("body") != "catalog benchmark":
                    raise RuntimeError(f"lookup mismatch: {value}")
                durations.append((time.perf_counter_ns() - start) / 1e6)
            return started, time.perf_counter_ns(), durations
        except BaseException:
            barrier.abort()
            raise
        finally:
            api.session.close()

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(worker, range(args.concurrency)))
    elapsed = (max(row[1] for row in results) - min(row[0] for row in results)) / 1e9
    durations = [duration for row in results for duration in row[2]]
    return {
        **summary(durations),
        "concurrency": args.concurrency,
        "elapsed_seconds": elapsed,
        "requests_per_second": len(durations) / elapsed,
    }


@contextmanager
def server(binary: Path, deployment: str):
    start = time.perf_counter()
    instance = (
        StandaloneAntflyServer(str(binary), "127.0.0.1", 0)
        if deployment == "standalone"
        else MultiNodeScalingCluster(str(binary), initial_data_node_count=3)
    )
    api = Api(
        antfly_public_api_url(instance.url, binary=str(binary))
        if deployment == "standalone"
        else instance.data_api_urls[0]
    )
    try:
        yield api, (time.perf_counter() - start) * 1000
    except Exception:
        print(instance.debug_logs()[-12000:], file=sys.stderr)
        raise
    finally:
        api.session.close()
        instance.stop()


def catalog_scenario(args, binary: Path) -> dict:
    with server(binary, args.deployment) as (api, startup):
        scope = "/databases/benchmark/namespaces/serving"
        api.request("POST", "/databases/benchmark", {})
        api.request("POST", scope, {})
        api.request(
            "POST",
            "/tablespaces/benchmark",
            {"placement_policy_json": json.dumps({"desired_replica_count": 1})},
        )
        api.request("PUT", scope + "/tablespace", {"tablespace_name": "benchmark"})
        previous = 0
        checkpoints = []
        for count in sorted(set(args.table_counts)):
            print(
                f"catalog: provisioning {count} tables ({args.deployment})",
                file=sys.stderr,
            )
            creates = []
            for i in range(previous, count):
                start = time.perf_counter_ns()
                api.request("POST", f"{scope}/tables/events_{i}", {"num_shards": 1})
                creates.append((time.perf_counter_ns() - start) / 1e6)
            previous = count
            table = f"events_{count - 1}"
            path = f"{scope}/tables/{table}"
            api.request(
                "POST",
                path + "/batch",
                {
                    "inserts": {
                        "doc": {"body": "catalog benchmark", "customer_id": "doc"}
                    },
                    "sync_level": "full_index",
                },
            )
            # A stable second table models enriching events with a customer
            # record and exercises binding distinct join destinations.
            if count > 1:
                api.request(
                    "POST",
                    scope + "/tables/events_0/batch",
                    {
                        "inserts": {"doc": {"body": "customer benchmark"}},
                        "sync_level": "full_index",
                    },
                )
            target = {"database": "benchmark", "namespace": "serving", "table": table}
            query = {
                "table_target": target,
                "full_text_search": {"match_all": {}},
                "limit": 10,
            }
            joined = {
                **query,
                "join": {
                    "right_target": {**target, "table": "events_0"},
                    "on": {"left_field": "customer_id", "right_field": "_id"},
                    "right_fields": ["body"],
                },
            }
            wire = "\n".join(json.dumps(query) for _ in range(args.ndjson_lines)) + "\n"

            def lookup(path=path):
                value = api.request("GET", path + "/documents/doc")
                if value.get("body") != "catalog benchmark":
                    raise RuntimeError(f"lookup mismatch: {value}")

            def run_query(body, count=count):
                value = api.request(
                    "POST", "/query", json.dumps(body) + "\n", ndjson=True
                )[0]
                if len(value["responses"][0]["hits"]["hits"]) != 1:
                    raise RuntimeError(f"query count mismatch: {value}")
                if "join" in body and count > 1:
                    source = value["responses"][0]["hits"]["hits"][0]["_source"]
                    if (
                        source.get("benchmark.serving.events_0.body")
                        != "customer benchmark"
                    ):
                        raise RuntimeError(f"join result mismatch: {source}")

            def run_ndjson(wire=wire):
                responses = api.request("POST", "/query", wire, ndjson=True)
                rows = [row for envelope in responses for row in envelope["responses"]]
                if len(rows) != args.ndjson_lines or any(
                    len(row["hits"]["hits"]) != 1 for row in rows
                ):
                    raise RuntimeError("NDJSON response count mismatch")

            def listing(count=count):
                rows = api.request("GET", scope + "/tables?prefix=events_")
                if len(rows) != count:
                    raise RuntimeError(f"listing count mismatch: {len(rows)}/{count}")

            operations = {
                "qualified_lookup": lookup,
                "qualified_query": lambda query=query: run_query(query),
                "qualified_join": lambda joined=joined: run_query(joined),
                "ndjson_repeated_target": run_ndjson,
                "scoped_listing": listing,
            }
            measured = {}
            for name, fn in operations.items():
                print(f"catalog: {count} tables, {name}", file=sys.stderr)
                measured[name] = api.measure(fn, args.samples, args.warmup)
            measured["concurrent_qualified_lookup"] = concurrent_lookups(
                api.base, path + "/documents/doc", args
            )
            identity = api.request("GET", path)["table_id"]
            current = [table]

            def rename(table=table, current=current):
                name = table + "_renamed" if current[0] == table else table
                api.request(
                    "POST", f"{scope}/tables/{current[0]}/rename", {"name": name}
                )
                current[0] = name

            measured["table_rename"] = api.measure(rename, args.samples, args.warmup)
            if (
                api.request("GET", f"{scope}/tables/{current[0]}")["table_id"]
                != identity
            ):
                raise RuntimeError("rename changed table identity")
            checkpoints.append(
                {
                    "table_count": count,
                    "table_create": summary(creates),
                    "operations": measured,
                }
            )
        return {
            "deployment": args.deployment,
            "startup_ms": startup,
            "checkpoints": checkpoints,
        }


def graph_nodes(response):
    graph = response["responses"][0].get("graph_results", {}).get("mentions", {})
    return graph.get("nodes", [])


def resolution_scenario(args, binary: Path) -> dict:
    with server(binary, "cluster") as (api, startup):
        api.request("POST", "/tables/entities", {"num_shards": 1})
        indexes = json.loads(json.dumps(DOCUMENTS_INDEXES))
        indexes["relations_graph"]["resolvers"][0]["candidate_search"] = "exact_key"
        api.request("POST", "/tables/documents", {"num_shards": 3, "indexes": indexes})
        checkpoints = []
        for mentions in sorted(set(args.mentions)):
            print(
                f"resolution: {mentions} mentions/document, {args.documents} documents",
                file=sys.stderr,
            )
            latencies = []
            polls = []
            for document in range(args.documents + args.warmup):
                key = f"{('1', '7', 'e')[document % 3]}:{mentions}:{document}"
                names = [f"Entity {mentions} {document} {i}" for i in range(mentions)]
                expected = {
                    "person/" + name.lower().replace(" ", "_") for name in names
                }
                # Half the mentions resolve existing entities; the rest mint new
                # ones. New keys per document make hydration prove promotion.
                if mentions // 2:
                    api.request(
                        "POST",
                        "/tables/entities/batch",
                        {
                            "inserts": {
                                "person/" + name.lower().replace(" ", "_"): {
                                    "entity_type": "person",
                                    "canonical_name": name,
                                    "aliases": [name],
                                }
                                for name in names[: mentions // 2]
                            },
                            "sync_level": "full_index",
                        },
                    )
                query = {
                    "query": {"match_all": {}},
                    "limit": 1,
                    "graph_queries": {
                        "mentions": {
                            "index": "relations_graph",
                            "traverse": {
                                "start": {"keys": [key]},
                                "edge_types": ["mentions"],
                                "max_depth": 1,
                                "limit": mentions + 1,
                                "include_documents": True,
                                "fields": ["canonical_name", "aliases"],
                            },
                        }
                    },
                }
                start = time.perf_counter()
                api.request(
                    "POST",
                    "/tables/documents/batch",
                    {
                        "inserts": {
                            key: {
                                "relations": {
                                    "entities": [
                                        {"id": f"e{i}", "label": "person", "text": name}
                                        for i, name in enumerate(names)
                                    ]
                                }
                            }
                        },
                        "sync_level": "write",
                    },
                )
                tries = 0
                while True:
                    tries += 1
                    value = api.request("POST", "/tables/documents/query", query)
                    hydrated = {
                        node["key"]
                        for node in graph_nodes(value)
                        if isinstance(node.get("document"), dict)
                    }
                    if expected <= hydrated:
                        break
                    if time.perf_counter() - start > args.readiness_timeout:
                        raise RuntimeError(
                            f"resolution timeout: {len(hydrated & expected)}/{mentions} entities"
                        )
                    time.sleep(args.poll_ms / 1000)
                if document >= args.warmup:
                    latencies.append((time.perf_counter() - start) * 1000)
                    polls.append(tries)
            graph_only = json.loads(json.dumps(query))
            graph_only["graph_queries"]["mentions"]["traverse"]["include_documents"] = (
                False
            )
            del graph_only["graph_queries"]["mentions"]["traverse"]["fields"]
            checkpoints.append(
                {
                    "mentions_per_document": mentions,
                    "existing_entities_per_document": mentions // 2,
                    "write_to_hydrated_graph": summary(latencies),
                    "readiness_poll_counts": polls,
                    "graph_topology_only": api.measure(
                        lambda graph_only=graph_only: api.request(
                            "POST", "/tables/documents/query", graph_only
                        ),
                        args.samples,
                        args.warmup,
                    ),
                    "graph_with_documents": api.measure(
                        lambda query=query: api.request(
                            "POST", "/tables/documents/query", query
                        ),
                        args.samples,
                        args.warmup,
                    ),
                }
            )
        return {
            "deployment": "3 metadata + 3 data nodes",
            "startup_ms": startup,
            "checkpoints": checkpoints,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ZIG_ROOT / "zig-out/bin/antfly")
    parser.add_argument(
        "--scenario", choices=["all", "catalog", "resolution"], default="all"
    )
    parser.add_argument(
        "--deployment",
        choices=["standalone", "cluster"],
        default="standalone",
        help="Catalog scenario deployment",
    )
    parser.add_argument("--table-counts", nargs="+", type=positive, default=[10, 100])
    parser.add_argument("--mentions", nargs="+", type=positive, default=[10, 100])
    parser.add_argument("--documents", type=positive, default=5)
    parser.add_argument("--concurrency", type=positive, default=8)
    parser.add_argument("--samples", type=positive, default=30)
    parser.add_argument("--warmup", type=positive, default=2)
    parser.add_argument("--ndjson-lines", type=positive, default=20)
    parser.add_argument("--poll-ms", type=positive, default=20)
    parser.add_argument("--readiness-timeout", type=positive, default=115)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    result = {
        "binary": str(binary),
        "binary_sha256": digest,
        "platform": platform.platform(),
        "settings": {
            k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()
        },
        "scenarios": {},
    }
    for name, run in [
        ("catalog", catalog_scenario),
        ("resolution", resolution_scenario),
    ]:
        if args.scenario in ("all", name):
            result["scenarios"][name] = run(args, binary)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
