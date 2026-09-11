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
from tempfile import TemporaryDirectory

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

    def diagnostics(self, path: str):
        try:
            return self.request("GET", path)
        except (requests.RequestException, RuntimeError, ValueError) as error:
            return {"diagnostic_request_error": str(error)}

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
    # E2E fixtures distinguish the Zig API root by executable basename.
    # Preserve arbitrary --binary paths without changing the measured binary.
    with TemporaryDirectory(prefix="antfly-catalog-binary-") as directory:
        alias = Path(directory) / "antfly"
        alias.symlink_to(binary)
        binary = alias
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
            yield api, (time.perf_counter() - start) * 1000, instance
        except Exception:
            print(instance.debug_logs()[-12000:], file=sys.stderr)
            raise
        finally:
            api.session.close()
            instance.stop()


def wait_for_catalog_shards(
    api: Api, instance, created: dict, args, path: str
) -> float:
    """Keep asynchronous shard bootstrap outside steady-state measurements."""
    if not isinstance(instance, MultiNodeScalingCluster):
        return 0.0
    start = time.perf_counter()
    deadline = start + args.readiness_timeout
    # A committed create may return a visibility-pending acknowledgement.
    # Observe the table with GET; never replay the mutation to obtain its body.
    observed = created
    while not observed.get("shards"):
        if time.perf_counter() >= deadline:
            raise RuntimeError(
                f"created table visibility timed out: {path}, {observed}"
            )
        try:
            observed = api.request("GET", path)
        except (requests.RequestException, RuntimeError) as error:
            observed = {"observation_error": str(error)}
        if not observed.get("shards"):
            time.sleep(min(args.poll_ms / 1000, max(0, deadline - time.perf_counter())))
    groups = {int(group) for group in observed["shards"]}
    pending = list(instance.metadata_urls)
    while pending:
        for url in pending.copy():
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                raise RuntimeError(
                    f"shard readiness timed out: {groups}, peers {pending}"
                )
            try:
                response = api.session.get(
                    url + "/metadata/v1/admin/snapshot", timeout=min(5, remaining)
                )
                response.raise_for_status()
                ready = {
                    int(status["group_id"])
                    for status in response.json().get("merged_group_statuses", [])
                    if status.get("leader_known")
                    and int(status.get("leader_store_id", 0)) != 0
                    and int(status.get("healthy_voter_reports", 0)) >= 1
                }
                if groups <= ready:
                    pending.remove(url)
            except requests.RequestException:
                # Only setup's read-only observation is retried.
                pass
        if pending:
            time.sleep(min(args.poll_ms / 1000, max(0, deadline - time.perf_counter())))
    return (time.perf_counter() - start) * 1000


def listing_table_config(args):
    table_config = {"num_shards": 1}
    if args.schema_fields:
        table_config["schema"] = {
            "document_schemas": {
                "default": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "body": {
                                "type": "string",
                                "x-antfly-types": ["text"],
                                "x-antfly-include-in-all": True,
                            },
                            **{
                                f"field_{field}": {"type": "string"}
                                for field in range(args.schema_fields)
                            },
                        },
                    }
                }
            }
        }
    return table_config


def catalog_scenario(args, binary: Path) -> dict:
    with server(binary, args.deployment) as (api, startup, instance):
        scope = "/databases/benchmark/namespaces/serving"
        api.request("POST", "/databases/benchmark", {})
        api.request("POST", scope, {})
        api.request(
            "POST",
            "/tablespaces/benchmark",
            {"placement_policy_json": json.dumps({"desired_replica_count": 1})},
        )
        api.request("PUT", scope + "/tablespace", {"tablespace_name": "benchmark"})
        table_config = listing_table_config(args)
        previous = 0
        checkpoints = []
        for count in sorted(set(args.table_counts)):
            print(
                f"catalog: provisioning {count} tables ({args.deployment})",
                file=sys.stderr,
            )
            creates = []
            shard_readiness = []
            for i in range(previous, count):
                start = time.perf_counter_ns()
                created = api.request(
                    "POST",
                    f"{scope}/tables/events_{i}",
                    table_config,
                )
                if args.schema_fields:
                    properties = created["schema"]["document_schemas"]["default"][
                        "schema"
                    ]["properties"]
                    if len(properties) != args.schema_fields + 1:
                        raise RuntimeError(
                            "table did not retain benchmark schema fields"
                        )
                creates.append((time.perf_counter_ns() - start) / 1e6)
                shard_readiness.append(
                    wait_for_catalog_shards(
                        api, instance, created, args, f"{scope}/tables/events_{i}"
                    )
                )
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
                    "shard_readiness": summary(shard_readiness),
                    "operations": measured,
                }
            )
        return {
            "deployment": args.deployment,
            "startup_ms": startup,
            "checkpoints": checkpoints,
        }


def management_scenario(args, binary: Path) -> dict:
    """Tenant discovery and DDL against an increasing unrelated inventory."""
    with server(binary, args.deployment) as (api, startup, instance):
        checkpoints = []
        created = 0
        serial = 0
        for count in sorted(set(args.tenant_counts)):
            print(
                f"management: provisioning {count} tenants", file=sys.stderr, flush=True
            )
            setup = []
            while created < count:
                start = time.perf_counter_ns()
                api.request("POST", f"/databases/tenant_{created}", {})
                setup.append((time.perf_counter_ns() - start) / 1e6)
                created += 1
            path = f"/databases/tenant_{count - 1}"
            identity = api.request("GET", path)["database_id"]

            def get(path=path, identity=identity):
                if api.request("GET", path)["database_id"] != identity:
                    raise RuntimeError("tenant identity changed")

            def listing(count=count):
                rows = api.request("GET", "/databases")
                if len(rows) != count + 1:
                    raise RuntimeError(f"database listing mismatch: {len(rows)}")

            def namespace_cycle(path=path):
                nonlocal serial
                serial += 1
                namespace = path + f"/namespaces/temporary_{serial}"
                api.request("POST", namespace, {})
                api.request("DELETE", namespace)

            def rename_cycle(path=path, identity=identity, count=count):
                api.request("POST", path + "/rename", {"name": "renamed_tenant"})
                renamed = api.request("GET", "/databases/renamed_tenant")
                if renamed["database_id"] != identity:
                    raise RuntimeError("rename changed tenant identity")
                api.request(
                    "POST",
                    "/databases/renamed_tenant/rename",
                    {"name": f"tenant_{count - 1}"},
                )

            # Distinct clients exercise catalog mutation serialization while
            # other clients perform point reads. Every mutation is checked.
            def mixed_worker(worker, path=path, identity=identity, count=count):
                client = Api(api.base)
                reads, writes = [], []
                try:
                    for i in range(args.samples):
                        start = time.perf_counter_ns()
                        if worker % 2:
                            namespace = f"/databases/tenant_0/namespaces/work_{count}_{worker}_{i}"
                            client.request("POST", namespace, {})
                            client.request("DELETE", namespace)
                            writes.append((time.perf_counter_ns() - start) / 1e6)
                        else:
                            if client.request("GET", path)["database_id"] != identity:
                                raise RuntimeError("concurrent tenant identity changed")
                            reads.append((time.perf_counter_ns() - start) / 1e6)
                    return reads, writes
                finally:
                    client.session.close()

            print(f"management: measuring {count} tenants", file=sys.stderr, flush=True)
            measured = {
                "point_get": api.measure(get, args.samples, args.warmup),
                "list_databases": api.measure(listing, args.samples, args.warmup),
                "namespace_create_drop": api.measure(
                    namespace_cycle, args.samples, args.warmup
                ),
                "rename_round_trip": api.measure(
                    rename_cycle, args.samples, args.warmup
                ),
            }
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                results = list(pool.map(mixed_worker, range(args.concurrency)))
            measured["concurrent_reads"] = summary(
                [x for reads, _ in results for x in reads]
            )
            writes = [x for _, writes in results for x in writes]
            if writes:
                measured["concurrent_namespace_create_drop"] = summary(writes)
            recovery = None
            if args.restart_after_ddl:
                # Exercise durable publication after mixed create/drop/rename
                # traffic. Recovery is reported separately from steady latency.
                identities = {
                    row["name"]: row["database_id"]
                    for row in api.request("GET", "/databases")
                }
                start = time.perf_counter_ns()
                instance.restart()
                recovered = {
                    row["name"]: row["database_id"]
                    for row in api.request("GET", "/databases")
                }
                if recovered != identities:
                    raise RuntimeError("catalog identities changed across restart")
                for tenant_path in {path, "/databases/tenant_0"}:
                    namespaces = api.request("GET", tenant_path + "/namespaces")
                    if {row["name"] for row in namespaces} != {"public"}:
                        raise RuntimeError(
                            "deleted namespaces reappeared after restart"
                        )
                recovery = {
                    "elapsed_ms": (time.perf_counter_ns() - start) / 1e6,
                    "verified_databases": len(recovered),
                }
            checkpoints.append(
                {
                    "tenant_count": count,
                    "restart_recovery": recovery,
                    "database_create": summary(setup),
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
    with server(binary, "cluster") as (api, startup, instance):
        entities = api.request(
            "POST", "/tables/entities", {"num_shards": args.entity_shards}
        )
        wait_for_catalog_shards(api, instance, entities, args, "/tables/entities")
        indexes = json.loads(json.dumps(DOCUMENTS_INDEXES))
        indexes["relations_graph"]["resolvers"][0]["candidate_search"] = (
            "prefix" if args.resolution_workload == "prefix" else "exact_key"
        )
        if args.entity_key_layout == "spread":
            indexes["relations_graph"]["resolvers"][0]["key_template"] = (
                "{{ slug _entity.text }}"
            )

        def entity_key(name):
            return (
                "" if args.entity_key_layout == "spread" else "person/"
            ) + name.lower().replace(" ", "_")

        documents = api.request(
            "POST", "/tables/documents", {"num_shards": 3, "indexes": indexes}
        )
        wait_for_catalog_shards(api, instance, documents, args, "/tables/documents")
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
                names = (
                    [
                        f"Repeated Person {i % min(mentions, 10)}"
                        for i in range(mentions)
                    ]
                    if args.resolution_workload == "prefix"
                    else [f"Entity {mentions} {document} {i}" for i in range(mentions)]
                )
                if args.entity_key_layout == "spread":
                    names = [
                        f"{i % (min(mentions, 10) if args.resolution_workload == 'prefix' else 16):x} {name}"
                        for i, name in enumerate(names)
                    ]
                expected = {entity_key(name) for name in names}
                seed = {
                    entity_key(name): {
                        "entity_type": "person",
                        "canonical_name": name,
                        "aliases": [name],
                    }
                    for name in names[: mentions // 2]
                }
                if args.resolution_workload == "redirects":
                    seed = {}
                    expected = set()
                    for name in names:
                        original = entity_key(name)
                        survivor = original + "_curated"
                        seed[original] = {
                            "entity_type": "person",
                            "canonical_name": name,
                            "merged_into": survivor,
                        }
                        seed[survivor] = {
                            "entity_type": "person",
                            "canonical_name": name,
                            "aliases": [name],
                        }
                        expected.add(survivor)
                if seed:
                    api.request(
                        "POST",
                        "/tables/entities/batch",
                        {"inserts": seed, "sync_level": "full_index"},
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
                            f"resolution timeout: {len(hydrated & expected)}/{len(expected)} destinations, "
                            f"document={key}, expected={sorted(expected)}, response={value}, "
                            f"index_status={api.diagnostics('/tables/documents/indexes/relations_graph')}"
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
                    "unique_entities": len(expected),
                    "workload": args.resolution_workload,
                    "seeded_entity_documents": len(seed),
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


def paged_inventory(api, path, expected, page_size):
    names = []
    cursor = None
    cursors = set()
    while True:
        params = {"limit": page_size}
        if cursor:
            params["cursor"] = cursor
        response = api.session.get(api.base + path, params=params, timeout=30)
        response.raise_for_status()
        rows = response.json()
        if len(rows) > page_size:
            raise RuntimeError("server did not bound the page")
        names.extend(row["name"] for row in rows)
        cursor = response.headers.get("X-Antfly-Next-Cursor")
        if not cursor:
            break
        if cursor in cursors:
            raise RuntimeError("cursor did not advance")
        cursors.add(cursor)
    if len(names) != expected or len(set(names)) != expected or names != sorted(names):
        raise RuntimeError("paged inventory omitted, duplicated, or reordered tables")


def inventory_with_readers(api, path, count, args):
    """One inventory scanner alongside concurrent schema-detail readers."""
    barrier = threading.Barrier(args.concurrency + 1, timeout=30)

    def worker(scanner):
        client = Api(api.base)
        timings = []
        try:
            barrier.wait()
            for _ in range(args.samples):
                start = time.perf_counter_ns()
                value = client.request("GET", path if scanner else path + "/needle")
                if scanner and len(value) != count:
                    raise RuntimeError("concurrent inventory mismatch")
                if not scanner and value["name"] != "needle":
                    raise RuntimeError("concurrent detail mismatch")
                timings.append((time.perf_counter_ns() - start) / 1e6)
            return timings
        finally:
            client.session.close()

    with ThreadPoolExecutor(max_workers=args.concurrency + 1) as pool:
        results = list(pool.map(worker, [True] + [False] * args.concurrency))
    return {
        "inventory": summary(results[0]),
        "detail": summary([t for result in results[1:] for t in result]),
        "readers": args.concurrency,
    }


def listing_scenario(args, binary: Path) -> dict:
    """Tenant discovery alongside unrelated, wide application schemas."""
    with server(binary, args.deployment) as (api, startup, instance):
        api.request("POST", "/databases/listing", {})
        small = "/databases/listing/namespaces/small"
        large = "/databases/listing/namespaces/large"
        for scope in (small, large):
            api.request("POST", scope, {})
        config = listing_table_config(args)
        api.request("POST", small + "/tables/selected", config)
        checkpoints = []
        previous = 0
        for count in sorted(set(args.table_counts)):
            print(f"listing: provisioning {count} unrelated tables", file=sys.stderr)
            for i in range(previous, count):
                name = "needle" if i == 0 else f"table_{i}"
                definition = json.loads(json.dumps(config))
                if args.listing_distinct_schemas and "schema" in definition:
                    definition["schema"]["document_schemas"]["default"]["schema"][
                        "properties"
                    ][f"application_{i}"] = {"type": "string"}
                created = api.request("POST", large + "/tables/" + name, definition)
                wait_for_catalog_shards(
                    api, instance, created, args, large + "/tables/" + name
                )
            previous = count

            def listing(path, expected):
                rows = api.request("GET", path)
                if len(rows) != expected:
                    raise RuntimeError(f"listing mismatch: {len(rows)} != {expected}")
                if path.startswith(small) and rows[0]["name"] != "selected":
                    raise RuntimeError("unrelated table leaked into small namespace")

            checkpoints.append(
                {
                    "unrelated_tables": count,
                    "small_namespace": api.measure(
                        lambda: listing(small + "/tables", 1), args.samples, args.warmup
                    ),
                    "selective_prefix": api.measure(
                        lambda: listing(large + "/tables?prefix=needle", 1),
                        args.samples,
                        args.warmup,
                    ),
                    "large_namespace": api.measure(
                        lambda count=count: listing(large + "/tables", count),
                        args.samples,
                        args.warmup,
                    ),
                    "single_table_status": api.measure(
                        lambda: api.request("GET", large + "/tables/needle"),
                        args.samples,
                        args.warmup,
                    ),
                    "empty_default_namespace": api.measure(
                        lambda: listing("/tables", 0), args.samples, args.warmup
                    ),
                }
            )
            if args.listing_page_size:
                checkpoints[-1]["first_page"] = api.measure(
                    lambda count=count: listing(
                        large + f"/tables?limit={args.listing_page_size}",
                        min(count, args.listing_page_size),
                    ),
                    args.samples,
                    args.warmup,
                )
                checkpoints[-1]["paged_inventory"] = api.measure(
                    lambda count=count: paged_inventory(
                        api, large + "/tables", count, args.listing_page_size
                    ),
                    args.samples,
                    args.warmup,
                )
            if args.listing_concurrent:
                checkpoints[-1]["inventory_with_readers"] = inventory_with_readers(
                    api, large + "/tables", count, args
                )
        return {
            "deployment": args.deployment,
            "startup_ms": startup,
            "checkpoints": checkpoints,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ZIG_ROOT / "zig-out/bin/antfly")
    parser.add_argument(
        "--scenario",
        choices=["all", "catalog", "management", "resolution", "listing"],
        default="all",
    )
    parser.add_argument(
        "--deployment",
        choices=["standalone", "cluster"],
        default="standalone",
        help="Catalog scenario deployment",
    )
    parser.add_argument(
        "--restart-after-ddl",
        action="store_true",
        help="Verify standalone catalog recovery after each management checkpoint",
    )
    parser.add_argument(
        "--listing-distinct-schemas",
        action="store_true",
        help="Use distinct definitions in the scoped listing workload",
    )
    parser.add_argument(
        "--listing-page-size",
        type=int,
        default=0,
        help="Also measure bounded pages and a validated complete keyset walk (0 disables)",
    )
    parser.add_argument(
        "--listing-concurrent",
        action="store_true",
        help="Run an inventory scanner alongside --concurrency detail readers",
    )
    parser.add_argument("--table-counts", nargs="+", type=positive, default=[10, 100])
    parser.add_argument(
        "--resolution-workload",
        choices=["exact", "prefix", "redirects"],
        default="exact",
    )
    parser.add_argument(
        "--schema-fields",
        type=int,
        default=0,
        help="Extra string fields per catalog table",
    )
    parser.add_argument("--tenant-counts", nargs="+", type=positive, default=[10, 100])
    parser.add_argument("--entity-shards", type=positive, default=1)
    parser.add_argument(
        "--entity-key-layout", choices=["clustered", "spread"], default="clustered"
    )
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
    if args.restart_after_ddl and args.deployment != "standalone":
        parser.error("--restart-after-ddl requires --deployment standalone")
    if args.listing_page_size < 0 or args.listing_page_size > 1000:
        parser.error("--listing-page-size must be between 0 and 1000")
    if args.schema_fields < 0:
        parser.error("--schema-fields must be nonnegative")
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
        ("listing", listing_scenario),
        ("resolution", resolution_scenario),
        ("management", management_scenario),
    ]:
        if args.scenario in ("all", name):
            result["scenarios"][name] = run(args, binary)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
