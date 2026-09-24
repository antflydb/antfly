#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Measure large-owner telemetry alongside tenant DDL on a healthy Raft cluster.

Run from zig/: uv run --project e2e/antfly python tools/benchmark_catalog_resilience.py
This measures the HTTP delivery path, not just serialization. Independent real
nodes must remain alive throughout. Warmup collections are explicitly discarded.
"""

import argparse
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from benchmark_system_catalog import ZIG_ROOT, positive, server, summary
from conftest import internal_service_headers
from test_catalog_resilience import read_pages, register_reporter


def wait_for_catalog_ready(api, cluster):
    """Exclude initial full-baseline apply and any resulting election from timing."""
    started = time.monotonic()
    deadline = started + 30
    stable_since = None
    previous = None
    attempts = 0
    last = None
    while time.monotonic() < deadline:
        attempts += 1
        statuses = cluster.metadata_statuses()
        leaders = [
            item
            for item in statuses
            if item.get("status", {}).get("metadata_raft_role") == "leader"
        ]
        identity = None
        if len(leaders) == 1:
            leader = leaders[0]
            term = leader["status"]["metadata_raft_term"]
            if all(
                item.get("status", {}).get("metadata_raft_term") == term
                for item in statuses
            ):
                identity = (leader["index"], term)
        try:
            response = api.session.get(
                api.base + "/databases/telemetry_bench", timeout=5
            )
            ready = response.ok
            last = {"status": response.status_code, "leader": identity}
        except requests.RequestException as error:
            ready = False
            last = {"error": str(error), "leader": identity}
        now = time.monotonic()
        if identity is not None and ready:
            if identity != previous or stable_since is None:
                stable_since = now
            elif now - stable_since >= 1:
                return identity[0], {
                    "elapsed_ms": (now - started) * 1000,
                    "attempts": attempts,
                    "leader_index": identity[0],
                    "term": identity[1],
                }
        else:
            stable_since = None
        previous = identity
        time.sleep(0.1)
    raise RuntimeError(f"catalog did not become ready after baseline apply: {last}")


def run(args):
    with server(args.binary.resolve(), "cluster") as (api, startup, c):
        setup_started = time.perf_counter_ns()
        # Establish the tenant and catalog protocol before introducing the large
        # baseline. Bootstrap/apply work is separate from steady-state telemetry
        # and namespace mutation measurements; measured requests never retry.
        api.request("POST", "/databases/telemetry_bench", {})
        report, cursor, leader = register_reporter(c, args.groups)
        leader, readiness = wait_for_catalog_ready(api, c)
        views = []
        for control in (True, False):
            started = time.perf_counter_ns()
            snapshot, sizes = read_pages(c, leader, control=control)
            views.append(
                {
                    "control": control,
                    "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
                    "bytes": sum(sizes),
                    "pages": len(sizes),
                    "max_page_bytes": max(sizes),
                }
            )
            del snapshot
        setup_ms = (time.perf_counter_ns() - setup_started) / 1e6
        results = []
        patch = {**report, "group_statuses": [], "runtime_statuses": []}
        sequence = 2
        for independent in (False, True):
            barrier = threading.Barrier(2, timeout=30)

            def telemetry(barrier=barrier, independent=independent):
                nonlocal sequence
                session = requests.Session()
                measured = []
                barrier.wait()
                try:
                    for sample in range(args.samples + args.warmup):
                        patch["embedding_activity_sequence"] += 1
                        started = time.perf_counter_ns()
                        for offset in range(0, args.groups, 512):
                            samples = [
                                {
                                    "group_id": 100000 + i,
                                    "index_name": "dense",
                                    "index_kind": "embeddings",
                                    "activity": {
                                        "epoch": 1,
                                        "sample_sequence": patch[
                                            "embedding_activity_sequence"
                                        ],
                                        "embeddings_computed": sample + 1,
                                    },
                                }
                                for i in range(offset, min(offset + 512, args.groups))
                            ]
                            response = session.post(
                                c.metadata_urls[leader]
                                + "/internal/v1/nodes/1000/status/update",
                                json={
                                    "telemetry_only": independent,
                                    "sequence": sequence,
                                    "base": cursor,
                                    "report": patch,
                                    "activity": samples,
                                },
                                headers=internal_service_headers(),
                                timeout=15,
                            )
                            response.raise_for_status()
                            assert response.json() == cursor
                            sequence += 1
                        elapsed = (time.perf_counter_ns() - started) / 1e6
                        if sample >= args.warmup:
                            measured.append(elapsed)
                    return measured
                finally:
                    session.close()

            def ddl(barrier=barrier, independent=independent):
                barrier.wait()
                measured = []
                for sample in range(args.samples + args.warmup):
                    path = f"/databases/telemetry_bench/namespaces/n_{int(independent)}_{sample}"
                    started = time.perf_counter_ns()
                    api.request("POST", path, {})
                    api.request("DELETE", path)
                    if sample >= args.warmup:
                        measured.append((time.perf_counter_ns() - started) / 1e6)
                return measured

            with ThreadPoolExecutor(max_workers=2) as pool:
                delivery = pool.submit(telemetry)
                mutations = pool.submit(ddl)
                times, ddl_times = delivery.result(), mutations.result()
            assert all(p.poll() is None for p in c.data_procs), c.debug_logs()
            results.append(
                {
                    "independent_telemetry": independent,
                    "collection": summary(times),
                    "collection_raw_ms": times,
                    "namespace_create_drop": summary(ddl_times),
                    "namespace_raw_ms": ddl_times,
                }
            )
        return {
            "startup_ms": startup,
            "setup_ms": setup_ms,
            "baseline_readiness": readiness,
            "metadata_nodes": 3,
            "real_data_nodes": 3,
            "groups": args.groups,
            "indexes_per_group": 1,
            "warmup": args.warmup,
            "snapshot_transfers": views,
            "scenarios": results,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ZIG_ROOT / "zig-out/bin/antfly")
    parser.add_argument("--groups", type=positive, default=10000)
    parser.add_argument("--samples", type=positive, default=12)
    parser.add_argument("--warmup", type=positive, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.binary.open("rb") as binary:
        digest = hashlib.file_digest(binary, "sha256").hexdigest()
    result = {"binary_sha256": digest, "workload": run(args)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
