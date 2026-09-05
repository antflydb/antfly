#!/usr/bin/env python3
"""Measure public vector queries while idempotent vector updates are ingesting.

The update workers rewrite existing VectorDBBench documents with their original
vectors. Ground truth therefore remains stable while the complete public write,
derived-index catch-up, native-WAL, publication, and query paths overlap.
"""

from __future__ import annotations

import argparse
import itertools
import json
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--table", default="vdbbench")
    parser.add_argument("--index", default="vec")
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--query-workers", type=int, default=10)
    parser.add_argument("--write-workers", type=int, default=4)
    parser.add_argument("--write-batch", type=int, default=100)
    parser.add_argument("--update-vectors", type=int, default=2000)
    parser.add_argument("--query-vectors", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--catchup-timeout", type=float, default=120.0)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def latency_summary(values: list[float]) -> dict[str, float]:
    return {
        "mean_ms": sum(values) / len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": max(values),
    }


def load_inputs(
    args: argparse.Namespace,
) -> tuple[list[list[float]], list[list[int]], list[tuple[int, list[float]]]]:
    tests = pq.ParquetFile(args.dataset / "test.parquet").read(columns=["emb"])["emb"]
    neighbors = pq.ParquetFile(args.dataset / "neighbors.parquet").read(
        columns=["neighbors_id"]
    )["neighbors_id"]
    train = pq.ParquetFile(args.dataset / "shuffle_train.parquet").read(
        columns=["id", "emb"]
    )
    query_count = min(args.query_vectors, len(tests), len(neighbors))
    update_count = min(args.update_vectors, len(train))
    if query_count <= 0 or update_count < args.write_batch:
        raise ValueError("dataset does not contain enough query/update vectors")
    query_vectors = [tests[i].as_py() for i in range(query_count)]
    query_neighbors = [neighbors[i].as_py()[: args.limit] for i in range(query_count)]
    train_ids = train["id"]
    train_vectors = train["emb"]
    updates = [
        (int(train_ids[i].as_py()), train_vectors[i].as_py())
        for i in range(update_count)
    ]
    return query_vectors, query_neighbors, updates


def index_ready(payload: dict[str, Any]) -> bool:
    status = (
        payload.get("status") if isinstance(payload.get("status"), dict) else payload
    )
    return (
        status.get("backfill_state") == "ready"
        and not bool(status.get("rebuilding", False))
        and not bool(status.get("dense_publish_pending", False))
        and int(status.get("query_visible_doc_count", 0))
        == int(status.get("doc_count", -1))
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.seconds <= 0 or args.query_workers <= 0 or args.write_workers <= 0:
        raise ValueError("seconds and worker counts must be positive")
    if args.write_batch <= 0 or args.update_vectors <= 0 or args.query_vectors <= 0:
        raise ValueError("batch and vector counts must be positive")

    query_vectors, query_neighbors, updates = load_inputs(args)
    base = f"http://{args.host}:{args.port}/db/v1/tables/{args.table}"
    query_url = f"{base}/query"
    batch_url = f"{base}/batch"
    index_url = f"{base}/indexes/{args.index}"
    barrier = threading.Barrier(args.query_workers + args.write_workers + 1)
    stop = threading.Event()
    errors: list[str] = []
    query_latencies: list[float] = []
    server_latencies: list[float] = []
    hbc_latencies: list[float] = []
    recalls: list[float] = []
    write_latencies: list[float] = []
    written_rows = 0
    write_lock = threading.Lock()
    update_cursor = itertools.count()

    def fail(kind: str, exc: Exception) -> None:
        errors.append(f"{kind}: {type(exc).__name__}: {exc}")
        stop.set()

    def query_worker(worker: int) -> None:
        query_index = worker % len(query_vectors)
        try:
            with httpx.Client(timeout=args.timeout) as client:
                barrier.wait(args.timeout)
                while not stop.is_set():
                    body = {
                        "embeddings": {args.index: query_vectors[query_index]},
                        "limit": args.limit,
                        "fields": [],
                        "profile": True,
                    }
                    started = time.perf_counter()
                    response = client.post(query_url, json=body)
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    response.raise_for_status()
                    first = response.json()["responses"][0]
                    hits = (first.get("hits") or {}).get("hits") or []
                    dense_profile = (first.get("profile") or {}).get(
                        "dense_search"
                    ) or {}
                    actual = {int(hit["_id"].split(":", 1)[1]) for hit in hits}
                    expected = set(query_neighbors[query_index])
                    query_latencies.append(elapsed_ms)
                    server_latencies.append(
                        float(dense_profile.get("total_ns", 0) or 0) / 1_000_000
                    )
                    hbc_latencies.append(
                        float(dense_profile.get("hbc_search_ns", 0) or 0) / 1_000_000
                    )
                    recalls.append(len(actual & expected) / args.limit)
                    query_index = (query_index + args.query_workers) % len(
                        query_vectors
                    )
        except (
            httpx.HTTPError,
            KeyError,
            ValueError,
            IndexError,
            threading.BrokenBarrierError,
        ) as exc:
            fail("query", exc)

    def write_worker() -> None:
        nonlocal written_rows
        try:
            with httpx.Client(timeout=args.timeout) as client:
                barrier.wait(args.timeout)
                while not stop.is_set():
                    batch_number = next(update_cursor)
                    start = (batch_number * args.write_batch) % len(updates)
                    selected = [
                        updates[(start + i) % len(updates)]
                        for i in range(args.write_batch)
                    ]
                    inserts = {
                        f"key:{doc_id}": {
                            "id": doc_id,
                            "metadata": doc_id,
                            "vec_data": str(doc_id),
                            "_embeddings": {args.index: vector},
                        }
                        for doc_id, vector in selected
                    }
                    started = time.perf_counter()
                    response = client.post(
                        batch_url,
                        json={"inserts": inserts, "sync_level": "write"},
                    )
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    response.raise_for_status()
                    write_latencies.append(elapsed_ms)
                    with write_lock:
                        written_rows += len(selected)
        except (httpx.HTTPError, threading.BrokenBarrierError) as exc:
            fail("write", exc)

    threads = [
        threading.Thread(target=query_worker, args=(worker,), daemon=True)
        for worker in range(args.query_workers)
    ] + [
        threading.Thread(target=write_worker, daemon=True)
        for _ in range(args.write_workers)
    ]
    for thread in threads:
        thread.start()
    try:
        barrier.wait(args.timeout)
    except threading.BrokenBarrierError as exc:
        stop.set()
        for thread in threads:
            thread.join(args.timeout + 5)
        raise RuntimeError("mixed workload workers failed to become ready") from exc
    started = time.perf_counter()
    stop.wait(args.seconds)
    stop.set()
    for thread in threads:
        thread.join(args.timeout + 5)
    elapsed = time.perf_counter() - started
    if any(thread.is_alive() for thread in threads):
        errors.append("worker did not stop within its request timeout")
    if not query_latencies:
        errors.append("mixed workload completed no queries")
    if not write_latencies:
        errors.append("mixed workload completed no writes")

    catchup_started = time.perf_counter()
    final_status: dict[str, Any] = {}
    with httpx.Client(timeout=args.timeout) as client:
        deadline = catchup_started + args.catchup_timeout
        while time.perf_counter() < deadline:
            response = client.get(index_url)
            response.raise_for_status()
            final_status = response.json()
            if index_ready(final_status):
                break
            time.sleep(0.1)
        else:
            errors.append("index did not return to query-visible ready state")
    catchup_seconds = time.perf_counter() - catchup_started

    result: dict[str, Any] = {
        "dataset": str(args.dataset.resolve()),
        "duration_seconds": elapsed,
        "query_workers": args.query_workers,
        "write_workers": args.write_workers,
        "write_batch": args.write_batch,
        "query_count": len(query_latencies),
        "query_qps": len(query_latencies) / elapsed,
        "query_latency": latency_summary(query_latencies) if query_latencies else None,
        "server_latency": latency_summary(server_latencies)
        if server_latencies
        else None,
        "hbc_latency": latency_summary(hbc_latencies) if hbc_latencies else None,
        "recall": sum(recalls) / len(recalls) if recalls else None,
        "write_batches": len(write_latencies),
        "written_rows": written_rows,
        "write_rows_per_second": written_rows / elapsed,
        "write_latency": latency_summary(write_latencies) if write_latencies else None,
        "catchup_seconds": catchup_seconds,
        "final_status": final_status,
        "errors": errors,
    }
    return result


def main() -> int:
    args = parse_args()
    result = run(args)
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
