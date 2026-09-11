"""Offline global subgroup routing screen; NOT a production qualification.

Source-only balanced bisection mirrors the experimental trainer's shape, not
the live tree topology. Exact sample neighbors evaluate routing; they never
select groups. A fixed parent frontier is shared by control and treatments.
Whole-group overshoot counts as work. Representative precision is only an ANN
routing hint: these scores cannot certify pruning or replace exact completion.
"""

import argparse
import hashlib
import json
import struct
import time
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from probe_dense_subgroup_bounds import normalize, read_prefix


def mean_direction(rows):
    center = rows.mean(axis=0, dtype=np.float64)
    norm = np.linalg.norm(center)
    return (center / norm if norm else center).astype(np.float32)


def balanced_groups(rows, ids, groups):
    """Four Lloyd iterations per balanced split, deterministic canonical ties."""
    if groups == 1:
        return [np.sort(ids)]
    if groups > len(ids) or groups & (groups - 1):
        raise ValueError("groups must be a power of two no larger than row count")
    members = rows[ids]
    left = members[0].astype(np.float64)
    right = members[np.argmin(members @ left)].astype(np.float64)
    middle = len(ids) // 2
    for _ in range(4):
        scores = members @ (right - left)
        order = np.lexsort((ids, scores))
        left_ids, right_ids = ids[order[:middle]], ids[order[middle:]]
        left = mean_direction(rows[left_ids]).astype(np.float64)
        right = mean_direction(rows[right_ids]).astype(np.float64)
    # This numerical model uses BLAS and f32 means, not bit-identical Zig sums.
    return balanced_groups(rows, left_ids, groups // 2) + balanced_groups(
        rows, right_ids, groups // 2
    )


def quantize_i8(rows):
    scales = np.max(np.abs(rows), axis=1) / 127
    scales[scales == 0] = 1
    scaled = rows / scales[:, None]
    # Match Zig @round: halfway cases round away from zero, not NumPy rint's
    # ties-to-even. These are routing hints, not authoritative vector values.
    codes = (
        np.copysign(np.floor(np.abs(scaled).astype(np.float64) + 0.5), scaled)
        .clip(-127, 127)
        .astype(np.int8)
    )
    return codes, scales.astype(np.float32)


def exact_neighbors(rows, queries, top_k):
    result = []
    ids = np.arange(len(rows))
    for query in queries:
        scores = np.empty(len(rows), dtype=np.float64)
        for start in range(0, len(rows), 1024):
            members = normalize(rows[start : start + 1024].astype(np.float64))
            scores[start : start + len(members)] = members @ query.astype(np.float64)
        result.append(np.lexsort((ids, -scores))[:top_k])
    return np.asarray(result)


def select_whole_groups(order, sizes, budget):
    if not len(order) or not 0 < budget <= sizes[order].sum():
        raise ValueError("budget must be positive and within the frontier")
    cumulative = np.cumsum(sizes[order])
    end = np.searchsorted(cumulative, budget, side="left") + 1
    return order[:end]


def measure_selection(selected, sizes, neighbor_groups):
    return int(sizes[selected].sum()), float(np.isin(neighbor_groups, selected).mean())


def screen(rows, queries, parent_count=256, groups_per_parent=4, top_k=100):
    started = time.perf_counter()
    parents = balanced_groups(rows, np.arange(len(rows)), parent_count)
    parent_centers = np.asarray([mean_direction(rows[ids]) for ids in parents])
    groups = [
        group
        for ids in parents
        for group in balanced_groups(rows, ids, groups_per_parent)
    ]
    centers = np.asarray([mean_direction(rows[ids]) for ids in groups])
    training_seconds = time.perf_counter() - started
    sizes = np.asarray([len(ids) for ids in groups])
    parent_sizes = np.asarray([len(ids) for ids in parents])
    group_for_row = np.empty(len(rows), dtype=np.int32)
    for group, ids in enumerate(groups):
        group_for_row[ids] = group
    neighbors = group_for_row[exact_neighbors(rows, queries, top_k)]
    parent_scores = queries.astype(np.float64) @ parent_centers.astype(np.float64).T
    codes, scales = quantize_i8(centers)
    qcodes, qscales = quantize_i8(queries)
    # Compact precision is evaluated independently of kernel timing. Dequantized
    # int8 products are mathematically equivalent to an exact int32 SIMD dot.
    variants = {
        "f32": queries.astype(np.float64) @ centers.astype(np.float64).T,
        "f16": queries.astype(np.float64)
        @ centers.astype(np.float16).astype(np.float64).T,
        "i8": (qcodes.astype(np.float64) @ codes.astype(np.float64).T)
        * qscales[:, None]
        * scales[None, :],
    }
    observations = {}
    for query_index in range(len(queries)):
        parent_order = np.argsort(-parent_scores[query_index], kind="stable")
        # Multiple fixed budgets prevent tuning the frontier to ground truth.
        for frontier_fraction in (0.25, 0.5, 0.75, 1.0):
            chosen_parents = select_whole_groups(
                parent_order, parent_sizes, len(rows) * frontier_fraction
            )
            eligible = np.sort(
                (
                    chosen_parents[:, None] * groups_per_parent
                    + np.arange(groups_per_parent)
                ).ravel()
            )
            frontier_work = sizes[eligible].sum()
            baseline = measure_selection(eligible, sizes, neighbors[query_index])

            def record(policy, precision, fraction, selected):
                key = (frontier_fraction, policy, precision, fraction)
                work, recall = measure_selection(
                    selected, sizes, neighbors[query_index]
                )
                observations.setdefault(key, []).append(
                    (
                        work,
                        recall,
                        baseline[0],
                        baseline[1],
                        len(selected),
                        len(np.unique(selected // groups_per_parent)),
                        len(chosen_parents),
                    )
                )

            record("control", "f32", 1.0, eligible)
            for precision, all_scores in variants.items():
                scores = all_scores[query_index]
                global_order = eligible[np.argsort(-scores[eligible], kind="stable")]
                for fraction in (0.5, 0.625, 0.75, 0.875, 0.9375, 1.0):
                    selected = select_whole_groups(
                        global_order, sizes, frontier_work * fraction
                    )
                    record("global", precision, fraction, selected)
                selected = []
                for parent in chosen_parents:
                    ids = np.arange(
                        parent * groups_per_parent, (parent + 1) * groups_per_parent
                    )
                    selected.extend(
                        ids[np.argsort(-scores[ids], kind="stable")][
                            : max(1, groups_per_parent * 3 // 4)
                        ]
                    )
                record("per_leaf_quota", precision, 0.75, np.asarray(selected))
    reports = []
    for (frontier, policy, precision, fraction), values in observations.items():
        (
            work,
            recall,
            base_work,
            base_recall,
            selected_groups,
            selected_parents,
            frontier_parents,
        ) = np.asarray(values).T
        loss_pp = float(100 * np.mean(base_recall - recall))
        reports.append(
            {
                "frontier_fraction": frontier,
                "policy": policy,
                "precision": precision,
                "requested_fraction_of_frontier": fraction,
                "vectors_mean": float(work.mean()),
                "sample_neighbor_coverage": float(recall.mean()),
                "control_vectors_mean": float(base_work.mean()),
                "control_sample_neighbor_coverage": float(base_recall.mean()),
                "coverage_loss_pp": loss_pp,
                "work_reduction_fraction": float(1 - work.mean() / base_work.mean()),
                "within_one_pp": loss_pp <= 1 + 1e-9,
                "selected_groups_mean": float(selected_groups.mean()),
                "selected_parents_mean": float(selected_parents.mean()),
                "entire_parents_skipped_mean": float(
                    (frontier_parents - selected_parents).mean()
                ),
            }
        )
    return {
        "qualification": "offline source-trained geometry screen; NOT live topology, production recall, or latency",
        "rows": len(rows),
        "queries": len(queries),
        "dimensions": rows.shape[1],
        "top_k": top_k,
        "parent_count": parent_count,
        "groups_per_parent": groups_per_parent,
        "training_seconds": training_seconds,
        "source_sha256": hashlib.sha256(rows.tobytes()).hexdigest(),
        "query_sha256": hashlib.sha256(queries.tobytes()).hexdigest(),
        "representative_bytes": {
            "f32": centers.nbytes,
            "f16": centers.size * 2,
            "i8": centers.size + len(centers) * 4,
        },
        "treatments": reports,
    }, centers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--queries", type=int, default=128)
    parser.add_argument("--parents", type=int, default=256)
    parser.add_argument("--groups", type=int, default=4)
    parser.add_argument("--top-k", type=int, default=100)
    args = parser.parse_args()
    if not (
        1 <= args.top_k <= args.rows <= 65536
        and 1 <= args.queries <= 256
        and 1 <= args.parents <= 512
        and args.parents & (args.parents - 1) == 0
        and args.groups in (2, 4, 8, 16)
        and args.parents * args.groups <= args.rows
    ):
        parser.error("invalid or unbounded screen dimensions")
    with threadpool_limits(limits=1):
        rows = read_prefix(args.dataset / "shuffle_train.parquet", args.rows)
        queries = read_prefix(args.dataset / "test.parquet", args.queries)
        report, centers = screen(rows, queries, args.parents, args.groups, args.top_k)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Bounded, self-describing little-endian fixture consumed by the Zig kernel
    # benchmark. Generated evidence only; never a production file format.
    fixture = args.output.with_suffix(".reps")
    fixture.write_bytes(
        struct.pack("<4sIII", b"SGRP", len(centers), rows.shape[1], len(queries))
        + centers.astype("<f4").tobytes()
        + queries.astype("<f4").tobytes()
    )
    report.update(
        dataset=str(args.dataset.resolve()),
        blas_threads=1,
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        helper_sha256=hashlib.sha256(
            Path(__file__).with_name("probe_dense_subgroup_bounds.py").read_bytes()
        ).hexdigest(),
        fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "treatments"}
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
