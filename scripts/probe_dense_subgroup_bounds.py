"""Offline cosine subgroup feasibility probe, NOT a serving benchmark.

Train only on a bounded prefix of shuffled source rows. The exact sampled-corpus
top-k threshold is an oracle: surviving work is an optimistic pruning floor,
not achievable query work, latency, or full-corpus recall. No durable files or
live indexes are changed. Query rows never participate in partition training.
"""

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits


def normalize(rows):
    norms = np.linalg.norm(rows, axis=1, keepdims=True)
    if not np.isfinite(rows).all() or np.any(norms <= 0):
        raise ValueError("probe requires finite nonzero vectors")
    return rows / norms


def read_prefix(path, count):
    chunks = []
    remaining = count
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=1024, columns=["emb"], use_threads=False
    ):
        # Bounded batches avoid materializing the whole parquet corpus.
        rows = np.asarray(batch.column(0).slice(0, remaining).to_pylist(), np.float32)
        chunks.append(rows)
        remaining -= len(rows)
        if remaining <= 0:
            break
    if remaining:
        raise ValueError(
            f"{path}: requested {count} rows but found {count - remaining}"
        )
    return normalize(np.concatenate(chunks))


def partition(rows, groups, seed, iterations=5):
    """Bounded spherical trainer; sampled centroids, then streamed assignment."""
    groups = min(groups, len(rows))
    if groups == 1:
        return np.zeros(len(rows), dtype=np.int32)
    rng = np.random.default_rng(seed)
    training = rows[rng.choice(len(rows), min(len(rows), 4096), replace=False)]
    centers = training[rng.choice(len(training), groups, replace=False)].copy()
    for _ in range(iterations):
        labels = np.argmax(training @ centers.T, axis=1)
        for group in range(groups):
            members = training[labels == group]
            if len(members):
                center = np.mean(members, axis=0, dtype=np.float64)
                norm = np.linalg.norm(center)
                if norm > 0:
                    centers[group] = center / norm
    result = np.empty(len(rows), dtype=np.int32)
    for start in range(0, len(rows), 1024):
        result[start : start + 1024] = np.argmax(
            rows[start : start + 1024] @ centers.T, axis=1
        )
    return result


def cap_bounds(rows, queries):
    members = normalize(rows.astype(np.float64))
    center = np.mean(members, axis=0)
    norm = np.linalg.norm(center)
    if norm <= 1e-12:
        return np.zeros(len(queries)), 2.0
    center /= norm
    # Guard all floating-point arithmetic toward keeping, not pruning, members.
    radius = min(2.0, float(np.max(np.linalg.norm(members - center, axis=1))) + 1e-6)
    distance = np.clip(1 - queries @ center - 1e-6, 0, 2)
    cap_distance = radius * radius / 2
    lower = np.maximum(
        0,
        1
        - (1 - distance) * (1 - cap_distance)
        - np.sqrt(distance * (2 - distance))
        * np.sqrt(max(0, cap_distance * (2 - cap_distance)))
        - 2e-6,
    )
    lower[distance <= cap_distance] = 0
    return lower, radius


def probe(rows, queries, coarse_groups, subdivisions, top_k, seed):
    queries = normalize(queries.astype(np.float64))
    started = time.perf_counter()
    coarse = partition(rows, coarse_groups, seed)
    coarse_seconds = time.perf_counter() - started
    # Reference work is outside all reported training timings. Keep at most
    # 16 query rows of authoritative distances resident at once.
    thresholds = np.empty(len(queries))
    neighbors = np.empty((len(queries), top_k), dtype=np.int64)
    for start in range(0, len(queries), 16):
        distances = np.empty((len(queries[start : start + 16]), len(rows)))
        for offset in range(0, len(rows), 1024):
            members = normalize(rows[offset : offset + 1024].astype(np.float64))
            distances[:, offset : offset + 1024] = (
                1 - queries[start : start + 16] @ members.T
            )
        thresholds[start : start + 16] = np.partition(distances, top_k - 1, axis=1)[
            :, top_k - 1
        ]
        neighbors[start : start + 16] = np.argpartition(distances, top_k - 1, axis=1)[
            :, :top_k
        ]
    reports = []
    for split in subdivisions:
        started = time.perf_counter()
        groups = []
        for parent in range(coarse_groups):
            ids = np.flatnonzero(coarse == parent)
            if not len(ids):
                continue
            local = partition(rows[ids], split, seed + parent + 1)
            groups.extend(ids[local == child] for child in np.unique(local))
        partition_seconds = time.perf_counter() - started
        survivors = np.zeros(len(queries), dtype=np.int64)
        surviving_groups = np.zeros(len(queries), dtype=np.int64)
        zero_bounds = 0
        max_bound_violation = 0.0
        radii = []
        centers = []
        group_for_id = np.empty(len(rows), dtype=np.int32)
        for group, ids in enumerate(groups):
            group_for_id[ids] = group
            lower, radius = cap_bounds(rows[ids], queries)
            radii.append(radius)
            zero_bounds += int(np.count_nonzero(lower == 0))
            keep = lower <= thresholds
            survivors += keep * len(ids)
            surviving_groups += keep
            members = normalize(rows[ids].astype(np.float64))
            center = np.mean(members, axis=0)
            center_norm = np.linalg.norm(center)
            centers.append(center / center_norm if center_norm > 1e-12 else center * 0)
            exact_min = np.min(1 - queries @ members.T, axis=1)
            max_bound_violation = max(
                max_bound_violation, float(np.max(lower - exact_min))
            )
            if np.any(lower > exact_min + 1e-12):
                raise AssertionError(
                    "unsafe bound: could prune an authoritative neighbor"
                )
        # A separate approximate-routing quality curve, not certified pruning.
        # Keep whole groups until reaching each budget; count overshoot rather
        # than hiding it. Exact sample neighbors are used only for evaluation.
        ranked = np.argsort(1 - queries @ np.asarray(centers).T, axis=1, kind="stable")
        sizes = np.asarray([len(ids) for ids in groups])
        cumulative_work = np.cumsum(sizes[ranked], axis=1)
        hits = np.asarray(
            [np.bincount(group_for_id[ids], minlength=len(groups)) for ids in neighbors]
        )
        cumulative_hits = np.cumsum(np.take_along_axis(hits, ranked, axis=1), axis=1)
        routing_curve = []
        for fraction in (0.05, 0.1, 0.2, 0.25, 0.35, 0.5, 1.0):
            last = np.argmax(cumulative_work >= len(rows) * fraction, axis=1)
            query_ids = np.arange(len(queries))
            routing_curve.append(
                {
                    "requested_fraction": fraction,
                    "actual_vectors_mean": float(
                        np.mean(cumulative_work[query_ids, last])
                    ),
                    "sample_recall": float(
                        np.mean(cumulative_hits[query_ids, last]) / top_k
                    ),
                }
            )
        reports.append(
            {
                "subdivisions": split,
                "nonempty_groups": len(groups),
                "coarse_training_seconds": coarse_seconds,
                "subgroup_training_seconds": partition_seconds,
                "mean_members_per_group": len(rows) / len(groups),
                "mean_radius": float(np.mean(radii)),
                "zero_bound_fraction": zero_bounds / (len(groups) * len(queries)),
                "oracle_surviving_vectors_mean": float(np.mean(survivors)),
                "oracle_surviving_fraction": float(np.mean(survivors) / len(rows)),
                "oracle_surviving_groups_mean": float(np.mean(surviving_groups)),
                "bound_centroid_coordinates_per_query": len(groups) * rows.shape[1],
                "centroid_radius_metadata_bytes": len(groups) * (rows.shape[1] * 4 + 4),
                "max_bound_violation": max_bound_violation,
                "centroid_order_routing_curve": routing_curve,
            }
        )
    return {
        "qualification": "offline sampled-corpus oracle; NOT production latency or recall",
        "rows": len(rows),
        "dimensions": rows.shape[1],
        "queries": len(queries),
        "top_k": top_k,
        "seed": seed,
        "coarse_groups_requested": coarse_groups,
        "maximum_training_rows_per_partition": 4096,
        "source_sample_sha256": hashlib.sha256(rows.tobytes()).hexdigest(),
        "query_sample_sha256": hashlib.sha256(queries.tobytes()).hexdigest(),
        "oracle_threshold_mean": float(np.mean(thresholds)),
        "all_member_bounds_validated": True,
        "treatments": reports,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--queries", type=int, default=128)
    parser.add_argument("--coarse-groups", type=int, default=64)
    parser.add_argument("--subdivisions", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--seed", type=int, default=593)
    args = parser.parse_args()
    if not (
        1 <= args.top_k <= args.rows <= 65536
        and 1 <= args.queries <= 256
        and 1 <= args.coarse_groups <= min(args.rows, 256)
        and all(1 <= size <= 64 for size in args.subdivisions)
    ):
        parser.error("invalid or unbounded probe dimensions")
    with threadpool_limits(limits=1):
        rows = read_prefix(args.dataset / "shuffle_train.parquet", args.rows)
        queries = read_prefix(args.dataset / "test.parquet", args.queries)
        report = probe(
            rows, queries, args.coarse_groups, args.subdivisions, args.top_k, args.seed
        )
    report["dataset"] = str(args.dataset.resolve())
    report["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["blas_threads"] = 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
