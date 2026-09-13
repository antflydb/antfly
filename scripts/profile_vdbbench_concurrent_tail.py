"""Read-only, closed-loop public-API tail attribution (not official VDBBench QPS).

Pre-encode requests before timing; retain paired HTTP/server stage measurements.
Run with the VectorDBBench virtualenv. No effort/recall or server policy overrides.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from copy import copy
from pathlib import Path

import httpx
import pyarrow.parquet as pq
from profile_vdbbench_public_query import PROFILE_FIELDS, timing_summary

ADMISSION_FIELDS = (
    "hbc_admission_estimated_scan_bytes",
    "hbc_admission_selected_scan_bytes",
    "hbc_admission_peak_reserved_bytes",
    "hbc_admission_reservations",
    "hbc_admission_fallback_leaves",
    "hbc_leaf_scan_bytes",
)
IO_FIELDS = (
    "hbc_rerank_vector_physical_reads",
    "hbc_rerank_vector_physical_bytes",
    "hbc_rerank_vector_projection_reads",
    "hbc_rerank_vector_projection_borrows",
)


def summarize(samples: list[dict], elapsed: float, concurrency: int) -> dict:
    def cohort(rows: list[dict]) -> dict:
        work = {}
        for field in ADMISSION_FIELDS:
            values = [
                row["admission_work"][field]
                for row in rows
                if field in row.get("admission_work", {})
            ]
            work[field] = {
                "samples": len(values),
                "mean": sum(values) / len(values) if values else None,
            }
        return {
            "count": len(rows),
            "http_ms": timing_summary([row["http_ms"] for row in rows]),
            "outside_server_timer_ms": timing_summary(
                [row["http_ms"] - row["total_ns"] for row in rows]
            ),
            "server_stages_ms": {
                key: timing_summary([row[key] for row in rows])
                for key in PROFILE_FIELDS
            },
            "admission_work": work,
            "physical_io": {
                field: {
                    "samples": len(values),
                    "mean": sum(values) / len(values) if values else None,
                }
                for field in IO_FIELDS
                for values in [
                    [
                        row["physical_io"][field]
                        for row in rows
                        if field in row.get("physical_io", {})
                    ]
                ]
            },
        }

    tail_count = max(1, (len(samples) + 19) // 20)
    return {
        "diagnostic_only": True,
        "concurrency": concurrency,
        "elapsed_seconds": elapsed,
        "completed": len(samples),
        "qps": len(samples) / elapsed,
        "recall": sum(row["recall"] for row in samples) / len(samples),
        "approximate_vectors_mean": sum(row["approximate"] for row in samples)
        / len(samples),
        "exact_vectors_mean": sum(row["exact"] for row in samples) / len(samples),
        "all": cohort(samples),
        "http_slowest_5_percent": cohort(
            sorted(samples, key=lambda row: row["http_ms"])[-tail_count:]
        ),
        "server_slowest_5_percent": cohort(
            sorted(samples, key=lambda row: row["total_ns"])[-tail_count:]
        ),
        "slowest": sorted(samples, key=lambda row: row["http_ms"], reverse=True)[:20],
    }


_start_barrier = None


def init_worker(barrier) -> None:
    global _start_barrier
    _start_barrier = barrier


def process_worker(args: argparse.Namespace):
    return asyncio.run(run(args, raw=True))


def worker_arguments(args: argparse.Namespace) -> list[argparse.Namespace]:
    result = []
    for ordinal in range(args.processes):
        worker = copy(args)
        worker.query_offset = ordinal * args.count // args.processes
        result.append(worker)
    return result


async def run(args: argparse.Namespace, raw: bool = False):
    vectors = pq.read_table(args.dataset / "test.parquet", columns=["emb"])["emb"]
    neighbors = pq.read_table(
        args.dataset / "neighbors.parquet", columns=["neighbors_id"]
    )["neighbors_id"]
    if not 0 < args.count <= min(len(vectors), len(neighbors)):
        raise ValueError("count must select a nonempty range present in both datasets")
    bodies = [
        json.dumps(
            {
                "embeddings": {"vec": vectors[i].as_py()},
                "limit": 100,
                "fields": [],
                "profile": True,
            }
        ).encode()
        for i in range(args.count)
    ]
    expected = [set(neighbors[i].as_py()[:100]) for i in range(args.count)]
    url = f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/query"
    samples: list[dict] = []
    next_query = getattr(args, "query_offset", 0)
    limits = httpx.Limits(
        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
    )
    async with httpx.AsyncClient(timeout=30, limits=limits, trust_env=False) as client:
        # Establish one request's connectivity and fail before starting the clock.
        response = await client.post(
            url,
            content=bodies[next_query % args.count],
            headers={"content-type": "application/json"},
        )
        response.raise_for_status()
        if _start_barrier is not None:
            _start_barrier.wait(timeout=120)
        started = time.perf_counter()
        deadline = started + args.seconds

        async def worker() -> None:
            nonlocal next_query
            while time.perf_counter() < deadline:
                index = next_query % args.count
                next_query += 1
                begin = time.perf_counter()
                response = await client.post(
                    url,
                    content=bodies[index],
                    headers={"content-type": "application/json"},
                )
                http_ms = (time.perf_counter() - begin) * 1000
                response.raise_for_status()
                first = response.json()["responses"][0]
                profile = first["profile"]["dense_search"]
                # Missing timers must not silently look like zero service time.
                if "total_ns" not in profile:
                    raise ValueError("server omitted its total dense-query timer")
                actual = {
                    int(hit["_id"].split(":", 1)[1])
                    for hit in (first.get("hits") or {}).get("hits", [])
                }
                samples.append(
                    {
                        "index": index,
                        "http_ms": http_ms,
                        "recall": len(actual & expected[index]) / 100,
                        "approximate": profile.get("hbc_approx_vectors_scored", 0),
                        "exact": profile.get("hbc_exact_vectors_scored", 0),
                        "admission_work": {
                            key: profile[key]
                            for key in ADMISSION_FIELDS
                            if key in profile
                        },
                        "physical_io": {
                            key: profile[key] for key in IO_FIELDS if key in profile
                        },
                        **{
                            key: float(profile.get(key, 0) or 0) / 1e6
                            for key in PROFILE_FIELDS
                        },
                    }
                )

        async with asyncio.TaskGroup() as group:
            for _ in range(args.concurrency):
                group.create_task(worker())
        elapsed = time.perf_counter() - started
    if raw:
        return samples, elapsed
    return {
        "endpoint": url,
        "dataset": str(args.dataset),
        "query_count": args.count,
        "stage_value_units": "milliseconds (including keys ending in _ns)",
        **summarize(samples, elapsed, args.concurrency),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Independent client processes; concurrency is per process",
    )
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.concurrency <= 0 or args.seconds <= 0 or args.processes <= 0:
        parser.error("concurrency, processes and seconds must be positive")
    if args.processes == 1:
        result = asyncio.run(run(args))
    else:
        # Match VDBBench's independent client processes. One asyncio event loop
        # at C30 can itself bottleneck; its HTTP tail cannot establish a server
        # regression. Synchronize after data preparation and warm connectivity.
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(args.processes)
        with ProcessPoolExecutor(
            max_workers=args.processes,
            mp_context=context,
            initializer=init_worker,
            initargs=(barrier,),
        ) as pool:
            runs = list(pool.map(process_worker, worker_arguments(args)))
        samples = [sample for rows, _ in runs for sample in rows]
        result = {
            "endpoint": f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/query",
            "dataset": str(args.dataset),
            "query_count": args.count,
            "stage_value_units": "milliseconds (including keys ending in _ns)",
            **summarize(
                samples,
                max(elapsed for _, elapsed in runs),
                args.concurrency * args.processes,
            ),
        }
    result["client_processes"] = args.processes
    result["client_query_offsets"] = [a.query_offset for a in worker_arguments(args)]
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "completed",
                    "qps",
                    "recall",
                    "all",
                    "http_slowest_5_percent",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
