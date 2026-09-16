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

"""Compare fresh and migrated source ownership on one host, sequentially.

Requires numpy and requests. Retains databases, inputs, logs and receipts under
--root; never deletes existing runs. Run 50K before 1M. This is a qualification
screen, not a substitute for alternating repeated VectorDBBench promotion arms.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time

import numpy as np
import requests


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def disk_bytes(root):
    return sum(p.stat().st_blocks * 512 for p in root.rglob("*") if p.is_file())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument(
        "--order",
        nargs="+",
        choices=("fresh", "online", "offline"),
        default=["fresh", "online", "offline"],
    )
    parser.add_argument("--query-count", type=int, default=4096)
    parser.add_argument("--churn", type=int, default=1000)
    args = parser.parse_args()
    args.root = args.root.resolve()
    args.binary = args.binary.resolve(strict=True)
    args.root.mkdir(parents=True, exist_ok=False)
    if args.rows < 4 * args.churn or args.dimensions < 3:
        parser.error(
            "rows must be at least four times churn; dimensions must be at least three"
        )
    env = os.environ.copy()
    overrides = {
        k: v
        for k, v in env.items()
        if k.startswith(("ANTFLY_SOURCE_VECTOR_", "ANTFLY_DENSE_", "ANTFLY_HBC_"))
    }
    if overrides:
        raise SystemExit(
            f"clear experimental overrides before qualification: {sorted(overrides)}"
        )
    write_json(
        args.root / "configuration.json",
        {
            **{k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
            "binary_sha256": hashlib.file_digest(
                args.binary.open("rb"), "sha256"
            ).hexdigest(),
            "metric": "cosine",
            "seed": 728,
        },
    )
    vectors = np.memmap(
        args.root / "input.f32",
        dtype=np.float32,
        mode="w+",
        shape=(args.rows, args.dimensions),
    )
    rng = np.random.default_rng(728)
    for start in range(0, args.rows, 4096):
        block = rng.standard_normal(
            (min(4096, args.rows - start), args.dimensions), dtype=np.float32
        )
        block /= np.linalg.norm(block, axis=1, keepdims=True)
        vectors[start : start + len(block)] = block
    vectors.flush()
    queries = rng.standard_normal((32, args.dimensions), dtype=np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)
    # Compute exact post-churn neighbors with bounded score scratch. Updates
    # negate the first range; the following range is deleted in every arm.
    best_scores = np.full((len(queries), 10), -np.inf, dtype=np.float32)
    best_ids = np.zeros((len(queries), 10), dtype=np.int64)
    for start in range(0, args.rows, 8192):
        block = np.array(vectors[start : start + 8192])
        ids = np.arange(start, start + len(block))
        block[ids < args.churn] *= -1
        scores = queries @ block.T
        scores[:, (ids >= args.churn) & (ids < 2 * args.churn)] = -np.inf
        choices = np.broadcast_to(ids, scores.shape)
        scores = np.concatenate((best_scores, scores), axis=1)
        choices = np.concatenate((best_ids, choices), axis=1)
        top = np.argpartition(scores, -10, axis=1)[:, -10:]
        best_scores = np.take_along_axis(scores, top, axis=1)
        best_ids = np.take_along_axis(choices, top, axis=1)
    truth = [{f"doc:{i:09d}" for i in row} for row in best_ids]
    write_json(args.root / "ground_truth.json", [sorted(row) for row in truth])
    results = []

    for ordinal, mode in enumerate(args.order):
        arm = args.root / f"{ordinal}-{mode}"
        arm.mkdir()
        data = arm / "data"
        data.mkdir()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        url = f"http://127.0.0.1:{port}/db/v1"
        table = "migration_qualification"
        process = None
        log = (arm / "server.log").open("w")
        peak = [0]
        monitoring = threading.Event()

        def sample():
            while not monitoring.wait(0.5):
                proc = process
                if proc is not None and proc.poll() is None:
                    raw = subprocess.run(
                        ["ps", "-o", "rss=", "-p", str(proc.pid)],
                        capture_output=True,
                        text=True,
                    )
                    if raw.returncode == 0 and raw.stdout.strip():
                        peak[0] = max(peak[0], int(raw.stdout.strip()) * 1024)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        session = requests.Session()

        def api(method, path, body=None):
            response = session.request(method, url + path, json=body, timeout=300)
            if response.status_code >= 400:
                raise RuntimeError(
                    f"{method} {path}: {response.status_code} {response.text[:2000]}"
                )
            return response.json() if response.content else {}

        def start_server():
            nonlocal process, session
            session.close()
            session = requests.Session()
            process = subprocess.Popen(
                [
                    str(args.binary),
                    "standalone",
                    "--data-dir",
                    str(data),
                    "--port",
                    str(port),
                    "--health",
                    "false",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
            )
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"server exited: {arm / 'server.log'}")
                try:
                    api("GET", "/tables")
                    return
                except requests.ConnectionError:
                    time.sleep(0.1)
            raise TimeoutError("server start")

        def stop_server():
            nonlocal process
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise
            process = None

        def docs(start, count, changed=False):
            block = np.array(vectors[start : start + count])
            if changed:
                block *= -1
            return {
                f"doc:{i:09d}": {
                    "text": f"document {i} migration qualification",
                    "_embeddings": {"model": vector.tolist()},
                }
                for i, vector in zip(range(start, start + count), block)
            }

        def ready(count):
            deadline = time.monotonic() + 1800
            while time.monotonic() < deadline:
                state = api("GET", f"/tables/{table}/indexes/model").get("status", {})
                if (
                    state.get("total_indexed") == count
                    and state.get("query_visible_doc_count") == count
                ):
                    return state
                time.sleep(0.25)
            raise TimeoutError(f"readiness: {state}")

        def query(vector):
            return api(
                "POST",
                f"/tables/{table}/query",
                {
                    "embeddings": {"model": vector.tolist()},
                    "indexes": ["model"],
                    "limit": 10,
                },
            )

        def churn():
            for first in range(0, args.churn, 256):
                api(
                    "POST",
                    f"/tables/{table}/batch",
                    {
                        "inserts": docs(first, min(256, args.churn - first), True),
                        "sync_level": "write",
                    },
                )
            for first in range(args.churn, 2 * args.churn, 256):
                api(
                    "POST",
                    f"/tables/{table}/batch",
                    {
                        "deletes": [
                            f"doc:{i:09d}"
                            for i in range(first, min(first + 256, 2 * args.churn))
                        ],
                        "sync_level": "write",
                    },
                )

        result = {"mode": mode, "rows": args.rows, "dimensions": args.dimensions}
        print(f"starting {mode} rows={args.rows}", flush=True)
        try:
            start_server()
            api(
                "POST",
                f"/tables/{table}",
                {
                    "num_shards": 1,
                    "storage": {
                        "dense_embeddings": "vector_store"
                        if mode == "fresh"
                        else "primary_lsm"
                    },
                },
            )
            api(
                "POST",
                f"/tables/{table}/indexes/model",
                {
                    "type": "embeddings",
                    "external": True,
                    "dimension": args.dimensions,
                    "distance_metric": "cosine",
                },
            )
            started = time.monotonic()
            for first in range(0, args.rows, 256):
                api(
                    "POST",
                    f"/tables/{table}/batch",
                    {
                        "inserts": docs(first, min(256, args.rows - first)),
                        "sync_level": "write",
                    },
                )
                if first % 25000 < 256:
                    print(f"{mode} loaded {first}/{args.rows}", flush=True)
            result["ingest_seconds"] = time.monotonic() - started
            result["initial_readiness"] = ready(args.rows)
            result["initial_ready_seconds"] = time.monotonic() - started
            result["before"] = api("GET", f"/tables/{table}")
            started = time.monotonic()
            if mode == "online":
                migration_request = {
                    "job_id": "qualification",
                    "target": "vector_store",
                    "budget": {"batch_rows": 1024, "batch_bytes": 4194304},
                }
                state = api(
                    "POST",
                    f"/tables/{table}/storage/migrations",
                    migration_request,
                )
                # Fixed mutations after capture admission, including deletions.
                churn()
                ready(args.rows - args.churn)
                with (arm / "migration.jsonl").open("w") as progress:
                    for step in range(100000):
                        progress.write(json.dumps(state) + "\n")
                        progress.flush()
                        if state["phase"] == "complete":
                            break
                        if step % 16 == 0:
                            query(queries[step % len(queries)])
                        state = api(
                            "POST",
                            f"/tables/{table}/storage/migrations/qualification",
                            {
                                "action": "publish"
                                if state["phase"] == "ready"
                                else "step",
                            },
                        )
                    else:
                        raise TimeoutError("online migration steps")
                result["migration"] = state
            else:
                churn()
                ready(args.rows - args.churn)
                if mode == "offline":
                    stop_server()
                    with (arm / "migration.log").open("w") as progress:
                        process = subprocess.Popen(
                            [
                                str(args.binary),
                                "storage",
                                "migrate",
                                "--to",
                                "vector-store",
                                "--catalog",
                                str(data / "metadata/local-metadata.json"),
                                "--replica-root",
                                str(data / "data/replicas"),
                                "--table",
                                table,
                                "--job",
                                "qualification",
                            ],
                            stdout=progress,
                            stderr=subprocess.STDOUT,
                        )
                        code = process.wait(timeout=3600)
                        if code != 0:
                            raise RuntimeError(
                                f"offline migration exited {code}: {arm / 'migration.log'}"
                            )
                        process = None
                    start_server()
            result["migration_and_churn_seconds"] = time.monotonic() - started
            ready(args.rows - args.churn)
            # Capture file/flush debt at the start of queries, before later
            # maintenance or restart can hide migration-induced tiny runs.
            result["before_queries"] = api("GET", f"/tables/{table}")
            write_json(arm / "before-queries.json", result)
            recalls = []
            for vector, expected in zip(queries, truth):
                response = query(vector)
                actual = {
                    hit["_id"] for hit in response["responses"][0]["hits"]["hits"]
                }
                if any(
                    args.churn <= int(key.removeprefix("doc:")) < 2 * args.churn
                    for key in actual
                ):
                    raise AssertionError("deleted document returned after migration")
                recalls.append(len(actual & expected) / 10)
            result["recall_at_10"] = float(np.mean(recalls))
            payloads = [
                json.dumps(
                    {
                        "embeddings": {"model": vector.tolist()},
                        "indexes": ["model"],
                        "limit": 10,
                    }
                )
                for vector in queries
            ]
            local = threading.local()
            full_text_payload = json.dumps(
                {
                    "full_text_search": {"field": "text", "match": "qualification"},
                    "limit": 10,
                }
            )
            text_result = api(
                "POST", f"/tables/{table}/query", json.loads(full_text_payload)
            )
            if not text_result["responses"][0]["hits"]["hits"]:
                raise AssertionError("full-text corpus missing after migration")

            def measured(index):
                if not hasattr(local, "session"):
                    local.session = requests.Session()
                begin = time.monotonic()
                response = local.session.post(
                    url + f"/tables/{table}/query",
                    data=active_payloads[index % len(active_payloads)],
                    headers={"Content-Type": "application/json"},
                    timeout=120,
                )
                response.raise_for_status()
                return time.monotonic() - begin

            def measure_workload(label):
                cells = []
                for concurrency in (1, 8, 32):
                    with ThreadPoolExecutor(max_workers=concurrency) as pool:
                        list(pool.map(measured, range(128)))
                        begin = time.monotonic()
                        latencies = list(pool.map(measured, range(args.query_count)))
                        seconds = time.monotonic() - begin
                    cells.append(
                        {
                            "concurrency": concurrency,
                            "qps": args.query_count / seconds,
                            "p50_ms": float(np.percentile(latencies, 50) * 1000),
                            "p99_ms": float(np.percentile(latencies, 99) * 1000),
                        }
                    )
                    write_json(arm / f"{label}.json", cells)
                    print(
                        f"{mode} {label} c={concurrency} qps={cells[-1]['qps']:.1f}",
                        flush=True,
                    )
                return cells

            for measurement, active_payloads in (
                ("queries", payloads),
                ("full_text_queries", [full_text_payload]),
                (
                    "mixed_queries",
                    [
                        item
                        for payload in payloads
                        for item in (payload, full_text_payload)
                    ],
                ),
            ):
                result[measurement] = measure_workload(measurement)
            result["after"] = api("GET", f"/tables/{table}")
            stop_server()
            started = time.monotonic()
            start_server()
            ready(args.rows - args.churn)
            query(queries[0])
            result["warm_restart_seconds"] = time.monotonic() - started
            result["restart"] = api("GET", f"/tables/{table}")
            active_payloads = payloads
            result["restart_queries"] = measure_workload("restart_queries")
            reclaim_started = time.monotonic()
            deadline = reclaim_started + 180
            result["source_reclamation_complete"] = False
            while time.monotonic() < deadline:
                state = api("GET", f"/tables/{table}")
                stats = state.get("storage_status", {}).get("source_vectors", {})
                if (
                    stats
                    and stats.get("retained_payloads") == args.rows - args.churn
                    and stats.get("collection_pending_bytes") == 0
                ):
                    result["source_reclamation_complete"] = True
                    break
                time.sleep(1)
            result["source_reclamation_seconds"] = time.monotonic() - reclaim_started
            result["reclamation"] = state
            result["peak_rss_bytes"] = peak[0]
            stop_server()
            result["allocated_disk_bytes"] = disk_bytes(data)
            write_json(arm / "result.json", result)
            if not result["source_reclamation_complete"]:
                raise AssertionError(
                    "source reclamation did not reach the live payload count"
                )
            # This fixed corpus has tiny documents relative to 768-D vectors.
            # Catch inline-sized primary retention before qualifying larger arms;
            # the exact table/byte measurements remain available in result.json.
            if mode != "fresh" and args.dimensions >= 512:
                primary_bytes = state["storage_status"]["lsm"]["run_bytes"]
                if primary_bytes > args.rows * args.dimensions * 4 // 2:
                    raise AssertionError(
                        f"primary SSTables still occupy inline-payload-sized space: {primary_bytes}"
                    )
            results.append(result)
            write_json(args.root / "results.json", results)
            print(
                f"finished {mode}: recall={result['recall_at_10']:.3f} qps={result['queries']} disk={result['allocated_disk_bytes']}",
                flush=True,
            )
        except BaseException as error:
            result["error"] = repr(error)
            write_json(arm / "failure.json", result)
            raise
        finally:
            stop_server()
            monitoring.set()
            sampler.join()
            log.close()
            session.close()


if __name__ == "__main__":
    main()
