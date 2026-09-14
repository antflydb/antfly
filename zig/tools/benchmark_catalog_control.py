#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Large-owner recovery, report bursts and diagnostic/control interference.

Run from zig/: uv run --project e2e/antfly python tools/benchmark_catalog_control.py
--output result.json. Timed publications try each metadata endpoint at most once and retain every
attempt; their latency includes discovery. Snapshot measurements never retry. Both old and
new binaries can be measured with --baseline-mode full or chunked respectively;
compare chunked and batched on one binary to isolate transport batching.
"""

import argparse
import copy
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from benchmark_catalog_resilience import wait_for_catalog_ready
from benchmark_system_catalog import ZIG_ROOT, positive, server, summary
from catalog_baseline import (
    canonical_report,
    encode,
    next_baseline_request,
    plan_baseline,
    post_baseline,
)
from conftest import internal_service_headers
from test_catalog_resilience import read_pages, register_reporter


def run(args):
    def checkpoint(phase, **values):
        args.progress.update(phase=phase, **values)
        args.output.write_text(json.dumps(args.progress, indent=2) + "\n")
        print(f"catalog workload: {phase}", flush=True)

    checkpoint("startup")
    with server(args.binary.resolve(), "cluster") as (api, startup, cluster):
        api.request("POST", "/databases/telemetry_bench", {})
        header, cursor, leader = register_reporter(cluster, args.groups)
        leader, readiness = wait_for_catalog_ready(api, cluster)
        fixture_attempts = []
        deadline = time.monotonic() + 30
        while True:
            try:
                snapshot, _ = read_pages(
                    cluster, leader, control=False, number_lexemes=True
                )
                break
            except requests.HTTPError as error:
                fixture_attempts.append(str(error))
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        stored = next(s for s in snapshot["stores"] if s["store_id"] == 1000)
        if args.indexes_per_group > 1:
            for runtime in stored["runtime_statuses"]:
                template = runtime["indexes"][0]
                runtime["indexes"] = [
                    {**copy.deepcopy(template), "name": f"dense_{i}"}
                    for i in range(args.indexes_per_group)
                ]
        full_report = canonical_report(
            header, stored["group_statuses"], stored["runtime_statuses"]
        )
        full = encode({"sequence": 2, "report": full_report}).encode()
        baseline = {"full_request_bytes": len(full), "mode": args.baseline_mode}
        checkpoint(
            "baseline",
            baseline=baseline,
            initial_readiness=readiness,
            fixture_retries=fixture_attempts,
        )
        started = time.perf_counter_ns()
        if args.baseline_mode == "full":
            response = requests.post(
                cluster.metadata_urls[leader] + "/internal/v1/nodes/1000/status/update",
                data=full,
                headers={
                    **internal_service_headers(),
                    "Content-Type": "application/json",
                },
                timeout=20,
            )
            baseline.update(status=response.status_code, response=response.text[:200])
            if response.ok:
                cursor = response.json()
        else:
            manifest, chunks = plan_baseline(header, stored, 2)
            baseline["logical_chunks"] = len(chunks)
            baseline["fragment_chunks"] = sum("$fragment" in chunk for chunk in chunks)
            baseline["plan_ms"] = (time.perf_counter_ns() - started) / 1e6
            delivery_started = time.perf_counter_ns()
            request = manifest
            sizes, times, attempts = [], [], []
            baseline["attempts"] = attempts
            while True:
                tick = time.perf_counter_ns()
                leader, progress, size = post_baseline(cluster, request, attempts)
                times.append((time.perf_counter_ns() - tick) / 1e6)
                sizes.append(size)
                if len(sizes) % 16 == 0:
                    checkpoint(
                        "baseline",
                        completed_requests=len(sizes),
                        next_chunk=progress["next_chunk"],
                    )
                if progress["activated"]:
                    cursor = progress["cursor"]
                    break
                request = dict(manifest)
                if progress["collecting"]:
                    continue
                request = next_baseline_request(
                    manifest,
                    chunks,
                    progress["next_chunk"],
                    batch_size=8 if args.baseline_mode == "batched" else 1,
                )
            baseline.update(
                status=200,
                delivery_ms=(time.perf_counter_ns() - delivery_started) / 1e6,
                requests=len(times),
                http_attempts=len(attempts),
                total_attempt_request_bytes=sum(
                    row["request_bytes"] for row in attempts
                ),
                request_latency=summary(times),
                request_raw_ms=times,
                total_request_bytes=sum(sizes),
                max_request_bytes=max(sizes),
                activation_ms=times[-1],
            )
        baseline["elapsed_ms"] = (time.perf_counter_ns() - started) / 1e6
        checkpoint("snapshots", baseline=baseline)
        del full, snapshot
        leader, after_baseline = wait_for_catalog_ready(api, cluster)
        url = cluster.metadata_urls[leader]
        headers = {**internal_service_headers(), "Content-Type": "application/json"}

        def capture(control):
            tick = time.perf_counter_ns()
            response = requests.post(
                url + "/internal/v1/snapshots/read",
                json={"control": control},
                headers=headers,
                timeout=25,
            )
            return {
                "status": response.status_code,
                "elapsed_ms": (time.perf_counter_ns() - tick) / 1e6,
                "token": response.headers.get("X-Antfly-Snapshot-Token"),
                "bytes": response.headers.get("X-Antfly-Snapshot-Bytes"),
                "error": response.text[:300] if not response.ok else "",
            }

        def release(row):
            if row["token"] is not None:
                response = requests.post(
                    url + "/internal/v1/snapshots/read",
                    json={"token": int(row["token"]), "release": True},
                    headers=headers,
                    timeout=5,
                )
                assert response.status_code == 204

        idle, contended, diagnostics = [], [], []
        for _ in range(args.samples):
            row = capture(True)
            release(row)
            idle.append(row)
            barrier = threading.Barrier(5, timeout=10)

            def diagnostic(barrier=barrier):
                barrier.wait()
                return capture(False)

            with ThreadPoolExecutor(max_workers=4) as pool:
                pending = [pool.submit(diagnostic) for _ in range(4)]
                barrier.wait()
                time.sleep(0.15)
                row = capture(True)
                contended.append(row)
                completed = [future.result() for future in pending]
            for result in completed + [row]:
                release(result)
            diagnostics.append(completed)
        checkpoint(
            "bursts",
            idle_control=idle,
            contended_control=contended,
            diagnostics=diagnostics,
        )
        bursts = []
        sequence = cursor["sequence"] + 1
        for count in sorted({min(size, args.groups) for size in (32, 33, 128)}):
            samples = []
            for sample in range(args.warmup + args.samples):
                for item in stored["group_statuses"][:count]:
                    item["raft_term"] += 1
                for item in stored["runtime_statuses"][:count]:
                    item["doc_count"] += 1
                report = canonical_report(
                    header,
                    stored["group_statuses"][:count],
                    stored["runtime_statuses"][:count],
                )
                body = encode(
                    {"sequence": sequence, "base": cursor, "report": report}
                ).encode()
                tick = time.perf_counter_ns()
                attempts = []
                for endpoint in [url] + [
                    candidate for candidate in cluster.metadata_urls if candidate != url
                ]:
                    response = requests.post(
                        endpoint + "/internal/v1/nodes/1000/status/update",
                        data=body,
                        headers=headers,
                        timeout=15,
                    )
                    attempts.append(
                        {
                            "endpoint": endpoint,
                            "status": response.status_code,
                            "body": response.text[:160] if not response.ok else "",
                        }
                    )
                    if response.ok:
                        url = endpoint
                        break
                if not response.ok:
                    checkpoint(
                        "burst_failed",
                        bursts=bursts,
                        changed_groups=count,
                        attempts=attempts,
                    )
                response.raise_for_status()
                cursor = response.json()
                published = (time.perf_counter_ns() - tick) / 1e6
                view = capture(True)
                release(view)
                if sample >= args.warmup:
                    samples.append(
                        {
                            "publish_ms": published,
                            "attempts": attempts,
                            "control_ms": view["elapsed_ms"],
                            "control_status": view["status"],
                            "control_error": view["error"],
                            "request_bytes": len(body),
                        }
                    )
                sequence += 1
            bursts.append(
                {
                    "changed_groups": count,
                    "raw": samples,
                    "publish": summary([row["publish_ms"] for row in samples]),
                    "control": summary(
                        [
                            row["control_ms"]
                            for row in samples
                            if row["control_status"] == 200
                        ]
                    )
                    if any(row["control_status"] == 200 for row in samples)
                    else {"samples": 0, "p50_ms": None, "p95_ms": None, "max_ms": None},
                    "control_failures": sum(
                        row["control_status"] != 200 for row in samples
                    ),
                }
            )
        assert all(proc.poll() is None for proc in cluster.data_procs)
        return {
            "startup_ms": startup,
            "groups": args.groups,
            "indexes_per_group": args.indexes_per_group,
            "metadata_nodes": 3,
            "real_data_nodes": 3,
            "all_real_data_nodes_alive": True,
            "samples": args.samples,
            "warmup": args.warmup,
            "initial_readiness": readiness,
            "fixture_retries": fixture_attempts,
            "baseline": baseline,
            "after_baseline_readiness": after_baseline,
            "idle_control": idle,
            "contended_control": contended,
            "diagnostics": diagnostics,
            "bursts": bursts,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ZIG_ROOT / "zig-out/bin/antfly")
    parser.add_argument("--groups", type=positive, default=10000)
    parser.add_argument("--indexes-per-group", type=positive, default=1)
    parser.add_argument("--samples", type=positive, default=5)
    parser.add_argument("--warmup", type=positive, default=1)
    parser.add_argument(
        "--baseline-mode", choices=("full", "chunked", "batched"), default="batched"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.binary.open("rb") as binary:
        digest = hashlib.file_digest(binary, "sha256").hexdigest()
    args.progress = {"binary_sha256": digest}
    try:
        result = {"binary_sha256": digest, "workload": run(args)}
    except Exception as error:
        args.progress["error"] = str(error)
        args.output.write_text(json.dumps(args.progress, indent=2) + "\n")
        raise
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
