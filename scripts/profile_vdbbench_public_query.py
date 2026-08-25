#!/usr/bin/env python3
"""Profile VectorDBBench recall and latency through Antfly's public table API.

Run this with VectorDBBench's virtualenv so httpx and pyarrow are available.
Unlike the leaderboard runner, this records Antfly's per-query server profile,
which separates HTTP/client time from HBC traversal and exact-artifact reads.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq

PROFILE_FIELDS = (
    "total_ns",
    "index_lookup_ns",
    "hbc_search_ns",
    "hbc_runtime_txn_ns",
    "hbc_scratch_acquire_ns",
    "hbc_node_cache_lookup_ns",
    "hbc_quantized_cache_lookup_ns",
    "hbc_rerank_vector_load_ns",
    "hbc_rerank_artifact_read_ns",
    "hbc_rerank_artifact_decode_ns",
    "hbc_rerank_distance_ns",
    "doc_key_resolve_ns",
    "doc_ordinal_lookup_ns",
    "load_projected_document_ns",
    "postprocess_ns",
)

COUNT_PROFILE_FIELDS = (
    "hbc_nodes_visited",
    "hbc_leaves_explored",
    "hbc_approx_vectors_scored",
    "hbc_exact_vectors_scored",
    "hbc_reranked_vectors",
    "hbc_approx_candidate_count",
    "hbc_rerank_candidate_count",
    "hbc_rerank_batches",
    "hbc_rerank_max_batch_size",
    "hbc_rerank_candidates_skipped_by_bound",
    "hbc_ambiguous_top_k_pairs",
    "hbc_ambiguous_boundary_pairs",
    "hbc_ambiguous_distance_over_hits",
    "hbc_ambiguous_distance_under_hits",
    "hbc_top_k_count",
    "hbc_rerank_metadata_vectors_loaded",
    "hbc_rerank_lsm_cache_hits",
    "hbc_rerank_lsm_cache_misses",
    "hbc_rerank_artifact_cache_hits",
    "hbc_rerank_artifact_vectors_loaded",
)

FLOAT_PROFILE_FIELDS = (
    "hbc_min_distance_gap_top_k",
    "hbc_min_interval_gap_top_k",
    "hbc_boundary_tail_error_avg",
    "hbc_boundary_tail_error_max",
    "hbc_boundary_tail_distance_gap_avg",
    "hbc_boundary_tail_distance_gap_min",
    "hbc_boundary_tail_distance_gap_max",
    "hbc_boundary_tail_interval_gap_avg",
    "hbc_boundary_tail_interval_gap_min",
    "hbc_boundary_tail_interval_gap_max",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        required=True,
        help="Dataset directory containing test.parquet and neighbors.parquet",
    )
    parser.add_argument(
        "--port", type=int, required=True, help="Antfly public API port"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--table", default="vdbbench")
    parser.add_argument("--index", default="vec")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--search-effort", type=float)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--slowest", type=int, default=20)
    parser.add_argument(
        "--output", type=Path, help="Also write the JSON result to this path"
    )
    return parser.parse_args()


def percentile(values: list[float], fraction: float) -> float:
    """Return a linearly interpolated percentile, matching numpy's default."""
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def timing_summary(values: list[float]) -> dict[str, float]:
    return {
        "mean_ms": sum(values) / len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": max(values),
    }


def value_summary(values: list[float]) -> dict[str, float]:
    return {
        "mean": sum(values) / len(values),
        "min": min(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    vectors = pq.ParquetFile(args.dataset / "test.parquet").read(columns=["emb"])["emb"]
    neighbors = pq.ParquetFile(args.dataset / "neighbors.parquet").read(
        columns=["neighbors_id"]
    )["neighbors_id"]
    end = args.offset + args.count
    if args.offset < 0 or args.count <= 0 or end > len(vectors) or end > len(neighbors):
        raise ValueError(
            f"query range [{args.offset}, {end}) exceeds dataset lengths {len(vectors)}/{len(neighbors)}"
        )

    elapsed: list[float] = []
    recalls: list[float] = []
    widths: list[int] = []
    leaves: list[int] = []
    approximate: list[int] = []
    exact: list[int] = []
    profile_timings: dict[str, list[float]] = {field: [] for field in PROFILE_FIELDS}
    profile_values: dict[str, list[float]] = {
        field: [] for field in COUNT_PROFILE_FIELDS + FLOAT_PROFILE_FIELDS
    }
    full_rerank_due_to_threshold = 0
    samples: list[dict[str, Any]] = []
    url = f"http://{args.host}:{args.port}/db/v1/tables/{args.table}/query"

    with httpx.Client(timeout=args.timeout) as client:
        for query_index in range(args.offset, end):
            body: dict[str, Any] = {
                "embeddings": {args.index: vectors[query_index].as_py()},
                "limit": args.limit,
                "fields": [],
                "profile": True,
            }
            if args.search_effort is not None:
                body["search_effort"] = args.search_effort
            started = time.perf_counter()
            response = client.post(url, json=body)
            elapsed_ms = (time.perf_counter() - started) * 1000
            if not response.is_success:
                raise RuntimeError(
                    f"query {query_index} failed: {response.status_code} {response.text}"
                )

            first = response.json()["responses"][0]
            hits = (first.get("hits") or {}).get("hits") or []
            actual = {int(hit["_id"].split(":", 1)[1]) for hit in hits}
            expected = set(neighbors[query_index].as_py()[: args.limit])
            recall = len(actual & expected) / args.limit
            profile = (first.get("profile") or {}).get("dense_search") or {}

            elapsed.append(elapsed_ms)
            recalls.append(recall)
            widths.append(int(profile.get("resolved_search_width", 0)))
            leaves.append(int(profile.get("hbc_leaves_explored", 0)))
            approximate.append(int(profile.get("hbc_approx_vectors_scored", 0)))
            exact.append(int(profile.get("hbc_exact_vectors_scored", 0)))
            sample: dict[str, Any] = {
                "index": query_index,
                "elapsed_ms": elapsed_ms,
                "recall": recall,
            }
            for field in PROFILE_FIELDS:
                value_ms = float(profile.get(field, 0) or 0) / 1_000_000
                profile_timings[field].append(value_ms)
                sample[field.removesuffix("_ns") + "_ms"] = value_ms
            for field in COUNT_PROFILE_FIELDS:
                value = int(profile.get(field, 0) or 0)
                profile_values[field].append(float(value))
                sample[field] = value
            for field in FLOAT_PROFILE_FIELDS:
                value = float(profile.get(field, 0) or 0)
                profile_values[field].append(value)
                sample[field] = value
            if bool(profile.get("hbc_full_rerank_due_to_threshold", False)):
                full_rerank_due_to_threshold += 1
            samples.append(sample)

    return {
        "endpoint": url,
        "dataset": str(args.dataset.resolve()),
        "search_effort": args.search_effort,
        "count": args.count,
        "offset": args.offset,
        "limit": args.limit,
        "recall": sum(recalls) / len(recalls),
        **timing_summary(elapsed),
        "width": {"min": min(widths), "max": max(widths)},
        "leaves_mean": sum(leaves) / len(leaves),
        "approximate_vectors_mean": sum(approximate) / len(approximate),
        "exact_vectors_mean": sum(exact) / len(exact),
        "profile_values": {
            field: value_summary(values) for field, values in profile_values.items()
        },
        "full_rerank_due_to_threshold_count": full_rerank_due_to_threshold,
        "server_timings": {
            field: timing_summary(values) for field, values in profile_timings.items()
        },
        "slowest": sorted(
            samples, key=lambda sample: sample["elapsed_ms"], reverse=True
        )[: args.slowest],
    }


def main() -> int:
    args = parse_args()
    result = run(args)
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
