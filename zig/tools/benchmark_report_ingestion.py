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
"""Measure concurrent store reports alongside tenant DDL on real metadata Raft.

Synthetic non-live stores do not enter placement. Reports traverse HTTP,
three-member metadata replication, projection publication and telemetry admission.
Run from zig/: uv run --project e2e/antfly python tools/benchmark_report_ingestion.py
"""

import argparse
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from benchmark_system_catalog import ZIG_ROOT, Api, positive, server, summary
from conftest import internal_service_headers


def run(args):
    with server(args.binary.resolve(), "cluster") as (api, startup, instance):
        api.request("POST", "/databases/report_bench", {})

        def post(path, body):
            failures = []
            for url in instance.metadata_urls:
                response = requests.post(
                    url + path,
                    headers=internal_service_headers(),
                    json=body,
                    timeout=30,
                )
                if response.ok:
                    return response
                failures.append((url, response.status_code, response.text))
            raise RuntimeError(f"{path}: {failures}")

        states = []
        for offset in range(args.reporters):
            store_id = 1000 + offset
            post("/internal/v1/nodes", body={"node_id": store_id, "role": "data"})
            post(
                "/internal/v1/nodes",
                body={
                    "store_id": store_id,
                    "node_id": store_id,
                    "reporter_incarnation": 77,
                    "live": False,
                    "dense_native_storage_protocol_version": 1,
                },
            )
            # Registration returns acceptance before distributed apply completes.
            deadline = time.monotonic() + 15
            while True:
                stores = instance.metadata_snapshot().get("stores", [])
                if any(int(store["store_id"]) == store_id for store in stores):
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"store registration did not converge: {store_id}"
                    )
                time.sleep(0.02)
            report = {
                "store_id": store_id,
                "reporter_incarnation": 77,
                "status_generation": 1,
                "live": False,
                "dense_native_storage_protocol_version": 1,
                "embedding_activity_protocol_version": 2,
                "embedding_activity_sequence": 1,
                "group_statuses": [
                    {"group_id": store_id * 100000 + i, "raft_term": 1}
                    for i in range(args.groups)
                ],
                "runtime_statuses": [
                    {
                        "group_id": store_id * 100000 + i,
                        "store_id": store_id,
                        "node_id": store_id,
                        "indexes": [{"name": "dense", "kind": "embeddings"}],
                    }
                    for i in range(args.groups)
                ],
            }
            cursor = post(
                f"/internal/v1/nodes/{store_id}/status/update",
                body={"sequence": 1, "report": report},
            ).json()
            states.append((report, cursor))
        checkpoints = []
        for mode in ("runtime_changes", "activity_only"):
            barrier = threading.Barrier(args.reporters + 1, timeout=30)

            def reporter(offset, barrier=barrier, mode=mode):
                report, cursor = states[offset]
                sequence = cursor["sequence"] + 1
                durations = []
                barrier.wait()
                started = time.perf_counter()
                for sample in range(args.samples):
                    report["embedding_activity_sequence"] += 1
                    report["runtime_statuses"][0]["doc_count"] = report[
                        "runtime_statuses"
                    ][0].get("doc_count", 0) + (mode == "runtime_changes")
                    patch = {
                        **report,
                        "group_statuses": report["group_statuses"][:1]
                        if mode == "runtime_changes"
                        else [],
                        "runtime_statuses": report["runtime_statuses"][:1]
                        if mode == "runtime_changes"
                        else [],
                    }
                    activity = {
                        "epoch": 1,
                        "sample_sequence": report["embedding_activity_sequence"],
                        "embeddings_computed": sample + 1,
                    }
                    if args.wire == "previous":
                        samples = [
                            {
                                **report["runtime_statuses"][0],
                                "indexes": [
                                    {
                                        "name": "dense",
                                        "kind": "embeddings",
                                        "embedding_activity_observed": True,
                                        "embedding_activity": activity,
                                    }
                                ],
                            }
                        ]
                    else:
                        samples = [
                            {
                                "group_id": report["runtime_statuses"][0]["group_id"],
                                "index_name": "dense",
                                "index_kind": "embeddings",
                                "activity": activity,
                            }
                        ]
                    begin = time.perf_counter_ns()
                    cursor = post(
                        f"/internal/v1/nodes/{report['store_id']}/status/update",
                        body={
                            "sequence": sequence,
                            "base": cursor,
                            "report": patch,
                            "activity": samples,
                        },
                    ).json()
                    durations.append((time.perf_counter_ns() - begin) / 1e6)
                    sequence += 1
                states[offset] = (report, cursor)
                return durations, time.perf_counter() - started

            def ddl(barrier=barrier, mode=mode):
                client = Api(api.base)
                durations = []
                try:
                    barrier.wait()
                    for sample in range(args.samples):
                        path = f"/databases/report_bench/namespaces/{mode}_{sample}"
                        begin = time.perf_counter_ns()
                        client.request("POST", path, {})
                        client.request("DELETE", path)
                        durations.append((time.perf_counter_ns() - begin) / 1e6)
                    return durations
                finally:
                    client.session.close()

            with ThreadPoolExecutor(max_workers=args.reporters + 1) as pool:
                workers = [pool.submit(reporter, i) for i in range(args.reporters)]
                ddl_worker = pool.submit(ddl)
                results = [worker.result() for worker in workers]
                ddl_times = ddl_worker.result()
            durations = [value for times, _ in results for value in times]
            checkpoints.append(
                {
                    "mode": mode,
                    "reports": summary(durations),
                    "reports_per_second": len(durations)
                    / max(elapsed for _, elapsed in results),
                    "namespace_create_drop": summary(ddl_times),
                }
            )
        return {
            "startup_ms": startup,
            "reporters": args.reporters,
            "groups_per_reporter": args.groups,
            "wire": args.wire,
            "metadata_replicas": 3,
            "checkpoints": checkpoints,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ZIG_ROOT / "zig-out/bin/antfly")
    parser.add_argument("--reporters", type=positive, default=8)
    parser.add_argument("--groups", type=positive, default=100)
    parser.add_argument("--samples", type=positive, default=30)
    parser.add_argument("--wire", choices=["compact", "previous"], default="compact")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = {
        "binary_sha256": hashlib.file_digest(
            args.binary.open("rb"), "sha256"
        ).hexdigest(),
        "workload": run(args),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
