#!/usr/bin/env python3
"""Summarize SPFresh/HBC comparison JSONL rows.

The comparison runner emits very wide benchmark records so the fields can stay
lossless. This script keeps the optimization gate readable by reducing those
records to the recall, latency, foreground write-amplification, repair, backlog,
and read-locality counters that should drive the decision.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


DEFAULT_RESULT_FILE = (
    Path(__file__).resolve().parent.parent
    / "bench"
    / "results"
    / "spfresh-hbc-comparison"
    / "spfresh-hbc-comparison.jsonl"
)


WRITE_COLUMNS = [
    "label",
    "workload",
    "posting",
    "backend",
    "directory",
    "dataset_mode",
    "recall_mode",
    "posting_base_member_block_size",
    "flat_block_size",
    "flat_probe_count",
    "flat_block_probe_count",
    "skip_vector_store",
    "samples",
    "operation_vectors",
    "active_vectors",
    "vectors",
    "recall",
    "recall_total",
    "recall_delta",
    "post_write_exact_truth_build_ns",
    "post_write_exact_truth_cache_hit",
    "qps",
    "qps_vs_packed",
    "p95_ns",
    "p95_vs_packed",
    "warm_quant_internal_misses",
    "warm_quant_leaf_misses",
    "warm_query_read_ranges",
    "warm_query_read_bytes",
    "warm_query_read_bytes_per_range",
    "ns_per_vector",
    "fg_node_bytes",
    "fg_node_bytes_vs_packed",
    "storage_write_bytes",
    "storage_write_bytes_vs_packed",
    "storage_write_files",
    "storage_write_files_vs_packed",
    "storage_write_bytes_per_file",
    "storage_manifest_write_bytes",
    "storage_manifest_write_files",
    "storage_manifest_write_bytes_per_file",
    "storage_renames",
    "storage_delete_files",
    "storage_delete_trees",
    "storage_read_bytes",
    "storage_read_ranges",
    "storage_read_bytes_per_range",
    "logical_write_puts",
    "logical_write_puts_vs_packed",
    "lsm_run_bytes",
    "lsm_run_bytes_vs_packed",
    "lsm_l0_runs",
    "lsm_l0_runs_vs_packed",
    "lsm_runs",
    "lsm_runs_vs_packed",
    "posting_lsm_keys",
    "posting_lsm_bytes",
    "posting_mutations",
    "posting_lsm_keys_per_mutation",
    "posting_lsm_bytes_per_mutation",
    "fg_save_nodes",
    "fg_splits",
    "fg_non_maintenance_splits",
    "fg_split_input_members",
    "fg_split_input_overflow",
    "fg_delta_appends",
    "fg_delta_records",
    "fg_delta_records_per_append",
    "fg_delta_value_bytes",
    "fg_delta_legacy_value_bytes",
    "fg_delta_value_bytes_per_record",
    "fg_delta_value_bytes_vs_legacy",
    "posting_delta_records",
    "posting_delta_value_bytes",
    "posting_delta_value_bytes_per_record",
    "posting_base_value_bytes",
    "posting_base_fixed_width_value_bytes",
    "posting_base_value_bytes_vs_fixed_width",
    "posting_base_value_bytes_per_member",
    "posting_base_blocks",
    "posting_base_members_per_block",
    "posting_base_value_bytes_per_block",
    "posting_base_member_probe_calls",
    "posting_base_member_probe_blocks_seen",
    "posting_base_member_probe_blocks_skipped_by_max",
    "posting_base_member_probe_skip_rate",
    "posting_base_member_probe_blocks_decoded",
    "posting_base_member_probe_skipped_blocks_per_call",
    "posting_base_member_probe_decoded_blocks_per_call",
    "posting_base_member_probe_members_decoded",
    "posting_base_member_probe_decoded_members_per_call",
    "posting_base_decode_ns",
    "posting_base_decode_members",
    "posting_base_decode_ns_per_member",
    "posting_delta_replay_ns",
    "posting_delta_replay_records",
    "posting_delta_replay_ns_per_record",
    "search_scratch_allocations",
    "search_scratch_allocation_bytes",
    "search_scratch_retained_bytes",
    "fg_route_leaf_groups",
    "fg_route_items",
    "fg_route_items_per_leaf_group",
    "fg_fold_tail_keys",
    "repair_ns_per_vector",
    "repair_before_bulk_finish",
    "repair_limit_reached",
    "repair_remaining_dirty",
    "repair_remaining_delta_tails",
    "repair_remaining_overfull",
    "repair_remaining_at_capacity",
    "repair_remaining_max_over_capacity",
    "pre_repair_recall",
    "pre_repair_recall_delta",
    "pre_repair_qps",
    "pre_repair_qps_vs_packed",
    "pre_repair_p95_ns",
    "pre_repair_p95_vs_packed",
    "pre_repair_exact_truth_build_ns",
    "pre_repair_exact_truth_cache_hit",
    "pre_repair_overlay_cache_hit_rate",
    "post_setup_ns",
    "post_runtime_txn_ns",
    "post_scratch_acquire_ns",
    "post_upper_tree_pin_ns",
    "post_child_expand_ns",
    "post_leaf_score_ns",
    "post_overlay_ns",
    "post_overlay_calls",
    "post_overlay_materialized_members",
    "post_query_materialization_ns_per_posting",
    "post_query_materialization_ns_per_member",
    "post_rerank_ns",
    "repair_allow_overfull",
    "repair_overfull_limit",
    "repair_over_capacity_limit",
    "repair_reassign_budget",
    "repair_reassign",
    "repair_split",
    "repair_split_full",
    "repair_layout_limit",
    "repair_delta_tail_limit",
    "auto_max_postings",
    "auto_fold_delta_tails",
    "auto_min_delta_records_to_fold",
    "auto_min_tombstone_records_to_fold",
    "auto_min_delta_to_base_ratio_bps",
    "auto_delta_tail_limit",
    "auto_layout_limit",
    "auto_split_full",
    "auto_min_capacity_to_run",
    "auto_boundary_reassignments",
    "auto_allow_overfull",
    "auto_overfull_limit",
    "auto_over_capacity_limit",
    "auto_runs",
    "auto_max_repaired_observed",
    "auto_max_layout_observed",
    "auto_max_split_observed",
    "auto_max_merge_observed",
    "auto_max_reassign_observed",
    "auto_max_fold_records_observed",
    "maintenance_repaired",
    "maintenance_split",
    "split_input_members",
    "split_input_overflow",
    "bulk_leaf_rebuilds",
    "maintenance_reassign",
    "maintenance_fold_records",
    "repair_fold_records",
    "repair_fold_tail_keys",
    "repair_fold_tail_value_bytes",
    "repair_fold_written_base_value_bytes",
    "repair_fold_peak_scratch_bytes",
    "repair_fold_peak_scratch_bytes_per_record",
    "repair_fold_peak_scratch_to_base_ratio",
    "repair_fold_peak_scratch_to_tail_ratio",
    "repair_fold_base_bytes_per_record",
    "repair_fold_base_tail_ratio",
    "backlog_dirty",
    "backlog_delta_tails",
    "backlog_overfull",
    "backlog_at_capacity",
    "backlog_max_over_capacity",
    "overlay_cache_bytes",
    "overlay_cache_entry_bytes",
    "overlay_delta_records",
    "post_write_query_rounds",
    "post_write_warm_queries",
    "overlay_cache_hits",
    "overlay_cache_misses",
    "overlay_cache_hit_rate",
    "overlay_cache_warm_hits",
    "overlay_cache_warm_misses",
    "overlay_cache_warm_hit_rate",
    "overlay_cache_evictions",
    "overlay_cache_admission_skips",
    "overlay_cache_member_bytes",
    "post_centroid_blocks_scanned",
    "post_centroid_blocks_selected",
    "post_block_probe_limit",
    "post_block_probe_count",
    "post_block_probe_limit_per_query",
    "post_block_probe_count_per_query",
    "post_block_centroids_scored",
    "post_block_centroid_estimates",
    "post_posting_centroids_scored",
    "post_posting_centroid_estimates",
]

READ_COLUMNS = [
    "label",
    "workload",
    "posting",
    "backend",
    "directory",
    "dataset_mode",
    "posting_base_member_block_size",
    "skip_vector_store",
    "samples",
    "vectors",
    "recall",
    "recall_total",
    "recall_delta",
    "qps",
    "qps_vs_packed",
    "ns_per_query",
    "p95_ns",
    "p95_vs_packed",
    "read_bytes",
    "read_bytes_vs_packed",
    "read_files",
    "read_ranges",
    "read_trailers",
    "file_size_calls",
    "read_bytes_per_range",
    "read_bytes_per_call",
    "cache_bytes",
    "workspace_bytes",
    "exact_truth_build_ns",
    "exact_truth_cache_hit",
    "overlay_cache_bytes",
    "overlay_cache_entry_bytes",
    "overlay_calls",
    "overlay_cache_hits",
    "overlay_cache_misses",
    "overlay_cache_hit_rate",
    "overlay_cache_evictions",
    "overlay_cache_admission_skips",
    "overlay_cache_member_bytes",
    "overlay_delta_records",
    "overlay_ns",
    "overlay_materialized_members",
    "query_materialization_ns_per_posting",
    "query_materialization_ns_per_member",
    "posting_base_decode_ns",
    "posting_base_decode_members",
    "posting_base_decode_ns_per_member",
    "posting_delta_replay_ns",
    "posting_delta_replay_records",
    "posting_delta_replay_ns_per_record",
    "centroid_blocks_scanned",
    "centroid_blocks_selected",
    "block_probe_limit",
    "block_probe_count",
    "block_centroids_scored",
    "block_centroid_estimates",
    "posting_centroids_scored",
    "posting_centroid_estimates",
    "approx_vectors",
    "exact_vectors",
    "profile_setup_ns",
    "profile_runtime_txn_ns",
    "profile_scratch_acquire_ns",
    "search_scratch_allocations",
    "search_scratch_allocation_bytes",
    "search_scratch_retained_bytes",
    "profile_upper_tree_pin_ns",
    "profile_child_expand_ns",
    "profile_leaf_score_ns",
    "profile_rerank_ns",
]


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
            if not isinstance(row, dict):
                raise SystemExit(f"{path}:{line_no}: expected JSON object")
            rows.append(row)
    return rows


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def mean(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if is_number(row.get(key))]
    if not values:
        return None
    return sum(values) / len(values)


def max_field(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if is_number(row.get(key))]
    if not values:
        return None
    return max(values)


def mean_limit(rows: list[dict[str, Any]], key: str) -> float | str | None:
    value = mean(rows, key)
    if value is None:
        return None
    if value >= float(1 << 63):
        return "unbounded"
    return value


def first(rows: list[dict[str, Any]], key: str) -> Any:
    for row in rows:
        if key in row:
            return row[key]
    return None


def directory_name(rows: list[dict[str, Any]]) -> str:
    explicit = first(rows, "centroid_directory")
    if explicit:
        return str(explicit)
    label = str(first(rows, "comparison_label") or "")
    if "two_level_rabitq" in label:
        return "two_level_rabitq"
    if "flat_rabitq" in label:
        return "flat_rabitq"
    return "hbc"


def ratio(value: float | None, baseline: float | None) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    return value / baseline


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    if not is_number(numerator) or not is_number(denominator):
        return None
    denominator_f = float(denominator)
    if denominator_f == 0:
        return None
    return float(numerator) / denominator_f


def numeric_sum(row: dict[str, Any], *keys: str) -> float | None:
    total = 0.0
    found = False
    for key in keys:
        value = row.get(key)
        if not is_number(value):
            continue
        total += float(value)
        found = True
    return total if found else None


def mean_numeric_sum(rows: list[dict[str, Any]], *keys: str) -> float | None:
    values = [value for row in rows if (value := numeric_sum(row, *keys)) is not None]
    if not values:
        return None
    return sum(values) / len(values)


def effective_two_level_block_probe_budget(row: dict[str, Any]) -> Any:
    observed = row.get("post_block_probe_limit_per_query")
    if is_number(observed) and float(observed) > 0:
        return observed
    return row.get("flat_block_probe_count")


def cache_hit_rate(hits: Any, misses: Any) -> float | None:
    if not is_number(hits) or not is_number(misses):
        return None
    return safe_ratio(hits, float(hits) + float(misses))


def preferred_mean(rows: list[dict[str, Any]], preferred_key: str, fallback_key: str) -> float | None:
    preferred = mean(rows, preferred_key)
    if preferred is not None:
        return preferred
    return mean(rows, fallback_key)


def preferred_mean_any(rows: list[dict[str, Any]], *keys: str) -> float | None:
    for key in keys:
        value = mean(rows, key)
        if value is not None:
            return value
    return None


def preferred_first(rows: list[dict[str, Any]], preferred_key: str, fallback_key: str) -> Any:
    preferred = first(rows, preferred_key)
    if preferred is not None:
        return preferred
    return first(rows, fallback_key)


def weighted_recall(rows: list[dict[str, Any]]) -> float | None:
    warm = weighted_post_write_recall(rows, "post_write_warm_recall_hits", "post_write_warm_recall_total")
    if warm is not None:
        return warm
    total = weighted_post_write_recall(rows, "post_write_recall_hits", "post_write_recall_total")
    if total is not None:
        return total
    return preferred_mean(rows, "post_write_warm_recall_at_k", "post_write_recall_at_k")


def weighted_recall_total(rows: list[dict[str, Any]]) -> float | None:
    warm = weighted_post_write_recall_total(rows, "post_write_warm_recall_total")
    if warm is not None:
        return warm
    return weighted_post_write_recall_total(rows, "post_write_recall_total")


def weighted_pre_repair_recall(rows: list[dict[str, Any]]) -> float | None:
    warm = weighted_post_write_recall(rows, "pre_repair_warm_recall_hits", "pre_repair_warm_recall_total")
    if warm is not None:
        return warm
    total = weighted_post_write_recall(rows, "pre_repair_recall_hits", "pre_repair_recall_total")
    if total is not None:
        return total
    return preferred_mean(rows, "pre_repair_warm_recall_at_k", "pre_repair_recall_at_k")


def weighted_post_write_recall(rows: list[dict[str, Any]], hits_key: str, total_key: str) -> float | None:
    hit_total = 0.0
    denom_total = 0.0
    for row in rows:
        hits = row.get(hits_key)
        denom = row.get(total_key)
        if is_number(hits) and is_number(denom):
            hit_total += float(hits)
            denom_total += float(denom)
    if denom_total > 0:
        return hit_total / denom_total
    return None


def weighted_post_write_recall_total(rows: list[dict[str, Any]], total_key: str) -> float | None:
    denom_total = 0.0
    for row in rows:
        denom = row.get(total_key)
        if is_number(denom):
            denom_total += float(denom)
    if denom_total > 0:
        return denom_total
    return None


def weighted_read_recall(rows: list[dict[str, Any]]) -> float | None:
    hit_total = 0.0
    denom_total = 0.0
    for row in rows:
        hits = row.get("recall_hits")
        denom = row.get("recall_total")
        if is_number(hits) and is_number(denom):
            hit_total += float(hits)
            denom_total += float(denom)
    if denom_total > 0:
        return hit_total / denom_total
    return mean(rows, "recall_at_k")


def weighted_read_recall_total(rows: list[dict[str, Any]]) -> float | None:
    denom_total = 0.0
    for row in rows:
        denom = row.get("recall_total")
        if is_number(denom):
            denom_total += float(denom)
    if denom_total > 0:
        return denom_total
    return None


def group_rows(rows: list[dict[str, Any]], kind: str) -> list[tuple[tuple[str, str], list[dict[str, Any]]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        label = str(row.get("comparison_label", ""))
        if kind == "write" and not label.startswith("write_"):
            continue
        if kind == "read" and not label.startswith("read_"):
            continue
        workload = str(row.get("workload", ""))
        groups[(label, workload)].append(row)
    return sorted(groups.items(), key=lambda item: (item[0][1], item[0][0]))


def baseline_by_workload(summaries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    baselines: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        if summary.get("posting") != "packed_hbc":
            continue
        if summary.get("directory") not in (None, "hbc"):
            continue
        workload = str(summary["workload"])
        current = baselines.get(workload)
        label = str(summary.get("label", ""))
        current_vectors = current.get("vectors") if current is not None else None
        candidate_vectors = summary.get("vectors")
        candidate_is_larger = current is None or (
            is_number(candidate_vectors)
            and (not is_number(current_vectors) or float(candidate_vectors) > float(current_vectors))
        )
        canonical_label = label == "write_packed_hbc_hbc" or label == "read_packed_hbc_hbc"
        current_label = str(current.get("label", "")) if current is not None else ""
        current_is_canonical = current_label == "write_packed_hbc_hbc" or current_label == "read_packed_hbc_hbc"
        same_scale_canonical = (
            current is not None
            and canonical_label
            and not current_is_canonical
            and is_number(candidate_vectors)
            and is_number(current_vectors)
            and float(candidate_vectors) == float(current_vectors)
        )
        if candidate_is_larger or same_scale_canonical:
            baselines[workload] = summary
    return baselines


def summarize_write(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for (_label, _workload), group in group_rows(rows, "write"):
        summaries.append(
            {
                "label": first(group, "comparison_label"),
                "workload": first(group, "workload"),
                "posting": first(group, "posting_storage"),
                "backend": first(group, "posting_backend"),
                "directory": directory_name(group),
                "dataset_mode": first(group, "dataset_mode"),
                "recall_mode": first(group, "post_write_recall_mode"),
                "posting_base_member_block_size": first(group, "posting_base_member_block_size"),
                "flat_block_size": first(group, "flat_centroid_block_size"),
                "flat_probe_count": first(group, "flat_centroid_probe_count"),
                "flat_block_probe_count": first(group, "flat_centroid_block_probe_count"),
                "skip_vector_store": first(group, "skip_vector_store"),
                "samples": len(group),
                "operation_vectors": mean(group, "vectors"),
                "active_vectors": preferred_mean(group, "active_count_after", "vectors"),
                "vectors": preferred_mean(group, "active_count_after", "vectors"),
                "recall": weighted_recall(group),
                "recall_total": weighted_recall_total(group),
                "post_write_exact_truth_build_ns": mean(group, "post_write_exact_truth_build_ns"),
                "post_write_exact_truth_cache_hit": first(group, "post_write_exact_truth_cache_hit"),
                "qps": preferred_mean(group, "post_write_warm_queries_per_second", "post_write_queries_per_second"),
                "p95_ns": preferred_mean(group, "post_write_warm_query_p95_ns", "post_write_query_p95_ns"),
                "warm_quant_internal_misses": preferred_mean(
                    group,
                    "post_write_warm_profile_quantized_internal_cache_misses",
                    "post_write_profile_quantized_internal_cache_misses",
                ),
                "warm_quant_leaf_misses": preferred_mean(
                    group,
                    "post_write_warm_profile_quantized_leaf_cache_misses",
                    "post_write_profile_quantized_leaf_cache_misses",
                ),
                "warm_query_read_ranges": preferred_mean(
                    group,
                    "post_write_warm_storage_read_range",
                    "post_write_storage_read_range",
                ),
                "warm_query_read_bytes": preferred_mean(
                    group,
                    "post_write_warm_storage_read_bytes",
                    "post_write_storage_read_bytes",
                ),
                "ns_per_vector": mean(group, "ns_per_vector"),
                "fg_node_bytes": preferred_mean(group, "foreground_ns_nodes_value_bytes", "ns_nodes_value_bytes"),
                "storage_write_bytes": mean(group, "storage_write_bytes"),
                "storage_write_files": mean(group, "storage_write_file"),
                "storage_manifest_write_bytes": mean(group, "storage_manifest_write_bytes"),
                "storage_manifest_write_files": mean(group, "storage_manifest_write_file"),
                "storage_renames": mean(group, "storage_rename"),
                "storage_delete_files": mean(group, "storage_delete_file"),
                "storage_delete_trees": mean(group, "storage_delete_tree"),
                "storage_read_bytes": mean(group, "storage_read_bytes"),
                "storage_read_ranges": mean(group, "storage_read_range"),
                "logical_write_puts": mean_numeric_sum(
                    group,
                    "ns_nodes_put_calls",
                    "ns_quant_put_calls",
                    "ns_vecs_put_calls",
                    "range_put_calls",
                    "posting_base_put_calls",
                    "posting_delta_append_calls",
                    "posting_state_put_calls",
                    "centroid_directory_put_calls",
                    "assignment_map_put_calls",
                ),
                "fg_save_nodes": preferred_mean(group, "foreground_save_node_calls", "save_node_calls"),
                "fg_splits": preferred_mean(group, "foreground_split_leaf_calls", "split_leaf_calls"),
                "fg_split_input_members": preferred_mean(
                    group,
                    "foreground_split_leaf_input_members_total",
                    "split_leaf_input_members_total",
                ),
                "fg_split_input_overflow": preferred_mean(
                    group,
                    "foreground_split_leaf_input_overflow_members_total",
                    "split_leaf_input_overflow_members_total",
                ),
                "fg_delta_appends": preferred_mean(
                    group,
                    "foreground_posting_delta_append_calls",
                    "posting_delta_append_calls",
                ),
                "fg_delta_records": preferred_mean(
                    group,
                    "foreground_posting_delta_records",
                    "posting_delta_records",
                ),
                "fg_delta_value_bytes": preferred_mean(
                    group,
                    "foreground_posting_delta_value_bytes",
                    "posting_delta_value_bytes",
                ),
                "posting_delta_records": mean(group, "posting_delta_records"),
                "posting_delta_value_bytes": mean(group, "posting_delta_value_bytes"),
                "posting_base_value_bytes": mean(group, "posting_base_value_bytes"),
                "posting_base_fixed_width_value_bytes": mean(group, "posting_base_fixed_width_value_bytes"),
                "posting_base_members": mean(group, "posting_base_members"),
                "posting_base_blocks": mean(group, "posting_base_blocks"),
                "posting_base_member_probe_calls": mean(group, "posting_base_member_probe_calls"),
                "posting_base_member_probe_blocks_seen": mean(group, "posting_base_member_probe_blocks_seen"),
                "posting_base_member_probe_blocks_skipped_by_max": mean(group, "posting_base_member_probe_blocks_skipped_by_max"),
                "posting_base_member_probe_blocks_decoded": mean(group, "posting_base_member_probe_blocks_decoded"),
                "posting_base_member_probe_members_decoded": mean(group, "posting_base_member_probe_members_decoded"),
                "posting_base_decode_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_base_decode_ns",
                    "post_write_profile_posting_base_decode_ns",
                ),
                "posting_base_decode_members": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_base_decode_members",
                    "post_write_profile_posting_base_decode_members",
                ),
                "posting_delta_replay_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_delta_replay_ns",
                    "post_write_profile_posting_delta_replay_ns",
                ),
                "posting_delta_replay_records": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_delta_replay_records",
                    "post_write_profile_posting_delta_replay_records",
                ),
                "search_scratch_allocations": preferred_mean(
                    group,
                    "post_write_warm_profile_search_scratch_allocations",
                    "post_write_profile_search_scratch_allocations",
                ),
                "search_scratch_allocation_bytes": preferred_mean(
                    group,
                    "post_write_warm_profile_search_scratch_allocation_bytes",
                    "post_write_profile_search_scratch_allocation_bytes",
                ),
                "search_scratch_retained_bytes": preferred_mean(
                    group,
                    "post_write_warm_profile_search_scratch_retained_bytes",
                    "post_write_profile_search_scratch_retained_bytes",
                ),
                "assignment_map_put_calls": mean(group, "assignment_map_put_calls"),
                "fg_route_leaf_groups": preferred_mean(
                    group,
                    "foreground_batch_route_leaf_groups",
                    "grouped_leaf_groups",
                ),
                "fg_route_items": preferred_mean(group, "foreground_batch_route_items", "grouped_items"),
                "fg_fold_tail_keys": preferred_mean(
                    group,
                    "foreground_posting_delta_fold_deleted_tail_keys",
                    "posting_delta_fold_deleted_tail_keys",
                ),
                "repair_ns_per_vector": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_ns_per_vector",
                    "posting_repair_after_write_ns_per_vector",
                ),
                "repair_before_bulk_finish": first(group, "repair_before_bulk_finish"),
                "repair_limit_reached": preferred_first(
                    group,
                    "posting_repair_before_bulk_finish_limit_reached",
                    "posting_repair_after_write_limit_reached",
                ),
                "repair_remaining_dirty": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_remaining_dirty_postings",
                    "posting_repair_after_write_remaining_dirty_postings",
                ),
                "repair_remaining_delta_tails": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_remaining_delta_tail_postings",
                    "posting_repair_after_write_remaining_delta_tail_postings",
                ),
                "repair_remaining_overfull": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_remaining_overfull_postings",
                    "posting_repair_after_write_remaining_overfull_postings",
                ),
                "repair_remaining_at_capacity": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_remaining_postings_at_capacity",
                    "posting_repair_after_write_remaining_postings_at_capacity",
                ),
                "repair_remaining_max_over_capacity": preferred_mean(
                    group,
                    "posting_repair_before_bulk_finish_remaining_max_over_capacity_members",
                    "posting_repair_after_write_remaining_max_over_capacity_members",
                ),
                "pre_repair_recall": weighted_pre_repair_recall(group),
                "pre_repair_qps": preferred_mean(
                    group,
                    "pre_repair_warm_queries_per_second",
                    "pre_repair_queries_per_second",
                ),
                "pre_repair_p95_ns": preferred_mean(
                    group,
                    "pre_repair_warm_query_p95_ns",
                    "pre_repair_query_p95_ns",
                ),
                "pre_repair_exact_truth_build_ns": mean(group, "pre_repair_exact_truth_build_ns"),
                "pre_repair_exact_truth_cache_hit": first(group, "pre_repair_exact_truth_cache_hit"),
                "pre_repair_overlay_cache_hits": preferred_mean(
                    group,
                    "pre_repair_warm_profile_posting_overlay_cache_hits",
                    "pre_repair_profile_posting_overlay_cache_hits",
                ),
                "pre_repair_overlay_cache_misses": preferred_mean(
                    group,
                    "pre_repair_warm_profile_posting_overlay_cache_misses",
                    "pre_repair_profile_posting_overlay_cache_misses",
                ),
                "post_setup_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_setup_ns",
                    "post_write_profile_setup_ns",
                ),
                "post_runtime_txn_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_runtime_txn_ns",
                    "post_write_profile_runtime_txn_ns",
                ),
                "post_scratch_acquire_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_scratch_acquire_ns",
                    "post_write_profile_scratch_acquire_ns",
                ),
                "post_upper_tree_pin_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_upper_tree_pin_ns",
                    "post_write_profile_upper_tree_pin_ns",
                ),
                "post_child_expand_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_child_expand_ns",
                    "post_write_profile_child_expand_ns",
                ),
                "post_leaf_score_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_leaf_score_ns",
                    "post_write_profile_leaf_score_ns",
                ),
                "post_overlay_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_ns",
                    "post_write_profile_posting_overlay_ns",
                ),
                "post_overlay_calls": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_calls",
                    "post_write_profile_posting_overlay_calls",
                ),
                "post_overlay_materialized_members": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_materialized_members",
                    "post_write_profile_posting_overlay_materialized_members",
                ),
                "post_rerank_ns": preferred_mean(
                    group,
                    "post_write_warm_profile_rerank_ns",
                    "post_write_profile_rerank_ns",
                ),
                "repair_allow_overfull": first(group, "repair_dirty_reassignment_allow_overfull"),
                "repair_overfull_limit": mean_limit(group, "repair_dirty_reassignment_max_overfull_postings"),
                "repair_over_capacity_limit": mean_limit(
                    group,
                    "repair_dirty_reassignment_max_over_capacity_members",
                ),
                "repair_reassign_budget": mean_limit(group, "repair_dirty_reassignments"),
                "repair_reassign": mean(group, "repair_posting_maintenance_boundary_reassigned_vectors"),
                "repair_split": mean(group, "repair_posting_maintenance_split_postings"),
                "repair_rebalance_layout": first(group, "repair_rebalance_layout"),
                "repair_split_full": first(group, "repair_split_full_postings"),
                "repair_layout_limit": mean_limit(group, "repair_max_layout_changes"),
                "repair_delta_tail_limit": mean_limit(group, "repair_max_delta_tail_postings"),
                "defer_leaf_splits_to_posting_maintenance": first(group, "defer_leaf_splits_to_posting_maintenance"),
                "auto_max_postings": mean_limit(group, "auto_posting_maintenance_max_postings"),
                "auto_fold_delta_tails": first(group, "auto_posting_maintenance_fold_delta_tails"),
                "auto_min_delta_records_to_fold": mean_limit(
                    group,
                    "auto_posting_maintenance_min_delta_records_to_fold",
                ),
                "auto_min_tombstone_records_to_fold": mean_limit(
                    group,
                    "auto_posting_maintenance_min_tombstone_records_to_fold",
                ),
                "auto_min_delta_to_base_ratio_bps": mean(
                    group,
                    "auto_posting_maintenance_min_delta_to_base_ratio_bps",
                ),
                "auto_delta_tail_limit": mean_limit(group, "auto_posting_maintenance_max_delta_tail_postings"),
                "auto_layout_limit": mean_limit(group, "auto_posting_maintenance_max_layout_changes"),
                "auto_split_full": first(group, "auto_posting_maintenance_split_full_postings"),
                "auto_min_capacity_to_run": mean_limit(group, "auto_posting_maintenance_min_postings_at_capacity_to_run"),
                "auto_boundary_reassignments": mean_limit(
                    group,
                    "auto_posting_maintenance_max_boundary_reassignments",
                ),
                "auto_allow_overfull": first(group, "auto_posting_maintenance_allow_overfull_reassignment"),
                "auto_overfull_limit": mean_limit(
                    group,
                    "auto_posting_maintenance_max_overfull_reassignment_postings",
                ),
                "auto_over_capacity_limit": mean_limit(
                    group,
                    "auto_posting_maintenance_max_over_capacity_reassignment_members",
                ),
                "auto_runs": mean(group, "auto_posting_maintenance_runs"),
                "auto_max_repaired_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_repaired_postings",
                ),
                "auto_max_layout_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_layout_changes",
                ),
                "auto_max_split_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_split_postings",
                ),
                "auto_max_merge_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_merged_postings",
                ),
                "auto_max_reassign_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_boundary_reassigned_vectors",
                ),
                "auto_max_fold_records_observed": max_field(
                    group,
                    "auto_posting_maintenance_observed_max_delta_fold_records",
                ),
                "maintenance_repaired": mean(group, "posting_maintenance_repaired_postings"),
                "maintenance_split": mean(group, "posting_maintenance_split_postings"),
                "split_input_members": mean(group, "split_leaf_input_members_total"),
                "split_input_overflow": mean(group, "split_leaf_input_overflow_members_total"),
                "bulk_leaf_rebuilds": mean(group, "bulk_leaf_rebuild_calls"),
                "maintenance_reassign": mean(group, "posting_maintenance_boundary_reassigned_vectors"),
                "maintenance_fold_records": mean(group, "posting_maintenance_delta_fold_records"),
                "repair_fold_records": mean(group, "repair_posting_maintenance_delta_fold_records"),
                "repair_fold_tail_keys": mean(group, "repair_posting_delta_fold_deleted_tail_keys"),
                "repair_fold_tail_value_bytes": mean(group, "repair_posting_delta_fold_deleted_tail_value_bytes"),
                "repair_fold_written_base_value_bytes": mean(
                    group,
                    "repair_posting_delta_fold_written_base_value_bytes",
                ),
                "repair_fold_peak_scratch_bytes": preferred_mean_any(
                    group,
                    "posting_repair_before_bulk_finish_delta_fold_peak_scratch_bytes",
                    "posting_repair_after_write_delta_fold_peak_scratch_bytes",
                    "repair_posting_delta_fold_peak_scratch_bytes",
                ),
                "backlog_dirty": mean(group, "posting_backlog_dirty_postings"),
                "backlog_delta_tails": mean(group, "posting_backlog_delta_tail_postings"),
                "backlog_overfull": mean(group, "posting_backlog_overfull_postings"),
                "backlog_at_capacity": mean(group, "posting_backlog_postings_at_capacity"),
                "backlog_max_over_capacity": mean(group, "posting_backlog_max_over_capacity_members"),
                "overlay_cache_bytes": mean(group, "max_posting_overlay_cache_bytes"),
                "overlay_cache_entry_bytes": mean(group, "max_posting_overlay_cache_entry_bytes"),
                "post_write_query_rounds": mean(group, "post_write_query_rounds"),
                "post_write_warm_queries": mean(group, "post_write_warm_queries"),
                "overlay_delta_records": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_delta_records",
                    "post_write_profile_posting_overlay_delta_records",
                ),
                "overlay_cache_warm_hits": mean(group, "post_write_warm_profile_posting_overlay_cache_hits"),
                "overlay_cache_warm_misses": mean(group, "post_write_warm_profile_posting_overlay_cache_misses"),
                "overlay_cache_hits": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_cache_hits",
                    "post_write_profile_posting_overlay_cache_hits",
                ),
                "overlay_cache_misses": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_cache_misses",
                    "post_write_profile_posting_overlay_cache_misses",
                ),
                "overlay_cache_evictions": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_cache_evictions",
                    "post_write_profile_posting_overlay_cache_evictions",
                ),
                "overlay_cache_admission_skips": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_cache_admission_skips",
                    "post_write_profile_posting_overlay_cache_admission_skips",
                ),
                "overlay_cache_member_bytes": preferred_mean(
                    group,
                    "post_write_warm_profile_posting_overlay_cache_member_bytes",
                    "post_write_profile_posting_overlay_cache_member_bytes",
                ),
                "post_centroid_blocks_scanned": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_blocks_scanned",
                    "post_write_centroid_directory_blocks_scanned",
                ),
                "post_centroid_blocks_selected": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_blocks_selected",
                    "post_write_centroid_directory_blocks_selected",
                ),
                "post_block_probe_limit": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_block_probe_limit",
                    "post_write_centroid_directory_block_probe_limit",
                ),
                "post_block_probe_count": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_block_probe_count",
                    "post_write_centroid_directory_block_probe_count",
                ),
                "post_block_centroids_scored": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_block_centroids_scored",
                    "post_write_centroid_directory_block_centroids_scored",
                ),
                "post_block_centroid_estimates": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_block_centroid_estimates",
                    "post_write_centroid_directory_block_centroid_estimates",
                ),
                "post_posting_centroids_scored": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_posting_centroids_scored",
                    "post_write_centroid_directory_posting_centroids_scored",
                ),
                "post_posting_centroid_estimates": preferred_mean(
                    group,
                    "post_write_warm_centroid_directory_posting_centroid_estimates",
                    "post_write_centroid_directory_posting_centroid_estimates",
                ),
                "lsm_runs": mean(group, "lsm_total_runs"),
                "lsm_l0_runs": mean(group, "lsm_l0_runs"),
                "lsm_run_bytes": mean(group, "lsm_total_run_bytes"),
                "posting_lsm_keys": mean_numeric_sum(
                    group,
                    "posting_base_put_calls",
                    "posting_delta_append_calls",
                ),
                "posting_lsm_bytes": mean_numeric_sum(
                    group,
                    "posting_base_key_bytes",
                    "posting_base_value_bytes",
                    "posting_delta_key_bytes",
                    "posting_delta_value_bytes",
                    "posting_delta_fold_deleted_tail_key_bytes",
                ),
                "posting_mutations": mean_numeric_sum(
                    group,
                    "posting_base_put_calls",
                    "posting_delta_records",
                ),
            }
        )

    baselines = baseline_by_workload(summaries)
    for summary in summaries:
        warm_queries = summary.get("post_write_warm_queries")
        summary["post_block_probe_limit_per_query"] = safe_ratio(summary.get("post_block_probe_limit"), warm_queries)
        summary["post_block_probe_count_per_query"] = safe_ratio(summary.get("post_block_probe_count"), warm_queries)
    for summary in summaries:
        summary["fg_non_maintenance_splits"] = non_negative_difference(
            summary.get("fg_splits"),
            summary.get("maintenance_split"),
        )
        baseline = baselines.get(str(summary["workload"]))
        summary["recall_delta"] = (
            None
            if baseline is None or summary["recall"] is None or baseline.get("recall") is None
            else summary["recall"] - baseline["recall"]
        )
        summary["qps_vs_packed"] = ratio(summary.get("qps"), baseline.get("qps") if baseline else None)
        summary["p95_vs_packed"] = ratio(summary.get("p95_ns"), baseline.get("p95_ns") if baseline else None)
        summary["pre_repair_recall_delta"] = (
            None
            if baseline is None or summary.get("pre_repair_recall") is None or baseline.get("recall") is None
            else summary["pre_repair_recall"] - baseline["recall"]
        )
        summary["pre_repair_qps_vs_packed"] = ratio(summary.get("pre_repair_qps"), baseline.get("qps") if baseline else None)
        summary["pre_repair_p95_vs_packed"] = ratio(summary.get("pre_repair_p95_ns"), baseline.get("p95_ns") if baseline else None)
        summary["fg_node_bytes_vs_packed"] = ratio(
            summary.get("fg_node_bytes"),
            baseline.get("fg_node_bytes") if baseline else None,
        )
        summary["storage_write_bytes_vs_packed"] = ratio(
            summary.get("storage_write_bytes"),
            baseline.get("storage_write_bytes") if baseline else None,
        )
        summary["storage_write_files_vs_packed"] = ratio(
            summary.get("storage_write_files"),
            baseline.get("storage_write_files") if baseline else None,
        )
        summary["storage_write_bytes_per_file"] = safe_ratio(
            summary.get("storage_write_bytes"),
            summary.get("storage_write_files"),
        )
        summary["storage_manifest_write_bytes_per_file"] = safe_ratio(
            summary.get("storage_manifest_write_bytes"),
            summary.get("storage_manifest_write_files"),
        )
        summary["storage_read_bytes_per_range"] = safe_ratio(
            summary.get("storage_read_bytes"),
            summary.get("storage_read_ranges"),
        )
        summary["warm_query_read_bytes_per_range"] = safe_ratio(
            summary.get("warm_query_read_bytes"),
            summary.get("warm_query_read_ranges"),
        )
        summary["logical_write_puts_vs_packed"] = ratio(
            summary.get("logical_write_puts"),
            baseline.get("logical_write_puts") if baseline else None,
        )
        summary["lsm_run_bytes_vs_packed"] = ratio(
            summary.get("lsm_run_bytes"),
            baseline.get("lsm_run_bytes") if baseline else None,
        )
        summary["lsm_runs_vs_packed"] = ratio(
            summary.get("lsm_runs"),
            baseline.get("lsm_runs") if baseline else None,
        )
        summary["lsm_l0_runs_vs_packed"] = ratio(
            summary.get("lsm_l0_runs"),
            baseline.get("lsm_l0_runs") if baseline else None,
        )
        summary["posting_lsm_keys_per_mutation"] = safe_ratio(
            summary.get("posting_lsm_keys"),
            summary.get("posting_mutations"),
        )
        summary["posting_lsm_bytes_per_mutation"] = safe_ratio(
            summary.get("posting_lsm_bytes"),
            summary.get("posting_mutations"),
        )
        summary["fg_delta_records_per_append"] = safe_ratio(
            summary.get("fg_delta_records"),
            summary.get("fg_delta_appends"),
        )
        if is_number(summary.get("fg_delta_appends")) and is_number(summary.get("fg_delta_records")):
            summary["fg_delta_legacy_value_bytes"] = float(summary["fg_delta_appends"]) * 9.0 + float(summary["fg_delta_records"]) * 17.0
        else:
            summary["fg_delta_legacy_value_bytes"] = None
        summary["fg_delta_value_bytes_per_record"] = safe_ratio(
            summary.get("fg_delta_value_bytes"),
            summary.get("fg_delta_records"),
        )
        summary["fg_delta_value_bytes_vs_legacy"] = ratio(
            summary.get("fg_delta_value_bytes"),
            summary.get("fg_delta_legacy_value_bytes"),
        )
        summary["posting_delta_value_bytes_per_record"] = safe_ratio(
            summary.get("posting_delta_value_bytes"),
            summary.get("posting_delta_records"),
        )
        summary["posting_base_value_bytes_vs_fixed_width"] = ratio(
            summary.get("posting_base_value_bytes"),
            summary.get("posting_base_fixed_width_value_bytes"),
        )
        summary["posting_base_value_bytes_per_member"] = safe_ratio(
            summary.get("posting_base_value_bytes"),
            summary.get("posting_base_members"),
        )
        summary["posting_base_members_per_block"] = safe_ratio(
            summary.get("posting_base_members"),
            summary.get("posting_base_blocks"),
        )
        summary["posting_base_value_bytes_per_block"] = safe_ratio(
            summary.get("posting_base_value_bytes"),
            summary.get("posting_base_blocks"),
        )
        summary["posting_base_member_probe_skip_rate"] = safe_ratio(
            summary.get("posting_base_member_probe_blocks_skipped_by_max"),
            summary.get("posting_base_member_probe_blocks_seen"),
        )
        summary["posting_base_member_probe_skipped_blocks_per_call"] = safe_ratio(
            summary.get("posting_base_member_probe_blocks_skipped_by_max"),
            summary.get("posting_base_member_probe_calls"),
        )
        summary["posting_base_member_probe_decoded_blocks_per_call"] = safe_ratio(
            summary.get("posting_base_member_probe_blocks_decoded"),
            summary.get("posting_base_member_probe_calls"),
        )
        summary["posting_base_member_probe_decoded_members_per_call"] = safe_ratio(
            summary.get("posting_base_member_probe_members_decoded"),
            summary.get("posting_base_member_probe_calls"),
        )
        summary["posting_base_decode_ns_per_member"] = safe_ratio(
            summary.get("posting_base_decode_ns"),
            summary.get("posting_base_decode_members"),
        )
        summary["posting_delta_replay_ns_per_record"] = safe_ratio(
            summary.get("posting_delta_replay_ns"),
            summary.get("posting_delta_replay_records"),
        )
        summary["post_query_materialization_ns_per_posting"] = safe_ratio(
            summary.get("post_overlay_ns"),
            summary.get("post_overlay_calls"),
        )
        summary["post_query_materialization_ns_per_member"] = safe_ratio(
            summary.get("post_overlay_ns"),
            summary.get("post_overlay_materialized_members"),
        )
        summary["fg_route_items_per_leaf_group"] = safe_ratio(
            summary.get("fg_route_items"),
            summary.get("fg_route_leaf_groups"),
        )
        summary["repair_fold_base_bytes_per_record"] = safe_ratio(
            summary.get("repair_fold_written_base_value_bytes"),
            summary.get("repair_fold_records"),
        )
        summary["repair_fold_base_tail_ratio"] = safe_ratio(
            summary.get("repair_fold_written_base_value_bytes"),
            summary.get("repair_fold_tail_value_bytes"),
        )
        summary["repair_fold_peak_scratch_bytes_per_record"] = safe_ratio(
            summary.get("repair_fold_peak_scratch_bytes"),
            summary.get("repair_fold_records"),
        )
        summary["repair_fold_peak_scratch_to_base_ratio"] = safe_ratio(
            summary.get("repair_fold_peak_scratch_bytes"),
            summary.get("repair_fold_written_base_value_bytes"),
        )
        summary["repair_fold_peak_scratch_to_tail_ratio"] = safe_ratio(
            summary.get("repair_fold_peak_scratch_bytes"),
            summary.get("repair_fold_tail_value_bytes"),
        )
        summary["overlay_cache_hit_rate"] = cache_hit_rate(
            summary.get("overlay_cache_hits"),
            summary.get("overlay_cache_misses"),
        )
        summary["overlay_cache_warm_hit_rate"] = cache_hit_rate(
            summary.get("overlay_cache_warm_hits"),
            summary.get("overlay_cache_warm_misses"),
        )
        summary["pre_repair_overlay_cache_hit_rate"] = cache_hit_rate(
            summary.get("pre_repair_overlay_cache_hits"),
            summary.get("pre_repair_overlay_cache_misses"),
        )
    return summaries


def summarize_read(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for (_label, _workload), group in group_rows(rows, "read"):
        summaries.append(
            {
                "label": first(group, "comparison_label"),
                "workload": first(group, "workload"),
                "posting": first(group, "posting_storage"),
                "backend": first(group, "posting_backend"),
                "directory": directory_name(group),
                "dataset_mode": first(group, "dataset_mode"),
                "posting_base_member_block_size": first(group, "posting_base_member_block_size"),
                "skip_vector_store": first(group, "skip_vector_store"),
                "samples": len(group),
                "vectors": mean(group, "vectors"),
                "recall": weighted_read_recall(group),
                "recall_total": weighted_read_recall_total(group),
                "qps": mean(group, "queries_per_second"),
                "ns_per_query": mean(group, "ns_per_query"),
                "p95_ns": mean(group, "query_p95_ns"),
                "read_bytes": mean(group, "storage_read_bytes"),
                "read_files": mean(group, "storage_read_file"),
                "read_ranges": mean(group, "storage_read_range"),
                "read_trailers": mean(group, "storage_read_trailer"),
                "file_size_calls": mean(group, "storage_file_size"),
                "cache_bytes": mean(group, "hbc_cache_total_bytes"),
                "workspace_bytes": mean(group, "search_workspace_bytes"),
                "exact_truth_build_ns": mean(group, "exact_truth_build_ns"),
                "exact_truth_cache_hit": first(group, "exact_truth_cache_hit"),
                "overlay_cache_bytes": mean(group, "max_posting_overlay_cache_bytes"),
                "overlay_cache_entry_bytes": mean(group, "max_posting_overlay_cache_entry_bytes"),
                "overlay_calls": mean(group, "profile_posting_overlay_calls"),
                "overlay_cache_hits": mean(group, "profile_posting_overlay_cache_hits"),
                "overlay_cache_misses": mean(group, "profile_posting_overlay_cache_misses"),
                "overlay_cache_evictions": mean(group, "profile_posting_overlay_cache_evictions"),
                "overlay_cache_admission_skips": mean(group, "profile_posting_overlay_cache_admission_skips"),
                "overlay_cache_member_bytes": mean(group, "profile_posting_overlay_cache_member_bytes"),
                "overlay_delta_records": mean(group, "profile_posting_overlay_delta_records"),
                "overlay_ns": mean(group, "profile_posting_overlay_ns"),
                "overlay_materialized_members": mean(group, "profile_posting_overlay_materialized_members"),
                "posting_base_decode_ns": mean(group, "profile_posting_base_decode_ns"),
                "posting_base_decode_members": mean(group, "profile_posting_base_decode_members"),
                "posting_delta_replay_ns": mean(group, "profile_posting_delta_replay_ns"),
                "posting_delta_replay_records": mean(group, "profile_posting_delta_replay_records"),
                "centroid_blocks_scanned": mean(group, "centroid_directory_blocks_scanned"),
                "centroid_blocks_selected": mean(group, "centroid_directory_blocks_selected"),
                "block_probe_limit": mean(group, "centroid_directory_block_probe_limit"),
                "block_probe_count": mean(group, "centroid_directory_block_probe_count"),
                "block_centroids_scored": mean(group, "centroid_directory_block_centroids_scored"),
                "block_centroid_estimates": mean(group, "centroid_directory_block_centroid_estimates"),
                "posting_centroids_scored": mean(group, "centroid_directory_posting_centroids_scored"),
                "posting_centroid_estimates": mean(group, "centroid_directory_posting_centroid_estimates"),
                "approx_vectors": mean(group, "approx_vectors_scored"),
                "exact_vectors": mean(group, "exact_vectors_scored"),
                "profile_setup_ns": mean(group, "profile_setup_ns"),
                "profile_runtime_txn_ns": mean(group, "profile_runtime_txn_ns"),
                "profile_scratch_acquire_ns": mean(group, "profile_scratch_acquire_ns"),
                "search_scratch_allocations": mean(group, "profile_search_scratch_allocations"),
                "search_scratch_allocation_bytes": mean(group, "profile_search_scratch_allocation_bytes"),
                "search_scratch_retained_bytes": mean(group, "profile_search_scratch_retained_bytes"),
                "profile_upper_tree_pin_ns": mean(group, "profile_upper_tree_pin_ns"),
                "profile_child_expand_ns": mean(group, "profile_child_expand_ns"),
                "profile_leaf_score_ns": mean(group, "profile_leaf_score_ns"),
                "profile_rerank_ns": mean(group, "profile_rerank_ns"),
            }
        )

    baselines = baseline_by_workload(summaries)
    for summary in summaries:
        baseline = baselines.get(str(summary["workload"]))
        summary["recall_delta"] = (
            None
            if baseline is None or summary["recall"] is None or baseline.get("recall") is None
            else summary["recall"] - baseline["recall"]
        )
        summary["qps_vs_packed"] = ratio(summary.get("qps"), baseline.get("qps") if baseline else None)
        summary["p95_vs_packed"] = ratio(summary.get("p95_ns"), baseline.get("p95_ns") if baseline else None)
        summary["read_bytes_vs_packed"] = ratio(
            summary.get("read_bytes"),
            baseline.get("read_bytes") if baseline else None,
        )
        summary["read_bytes_per_range"] = safe_ratio(
            summary.get("read_bytes"),
            summary.get("read_ranges"),
        )
        summary["read_bytes_per_call"] = safe_ratio(
            summary.get("read_bytes"),
            numeric_sum(summary, "read_files", "read_ranges", "read_trailers"),
        )
        summary["overlay_cache_hit_rate"] = cache_hit_rate(
            summary.get("overlay_cache_hits"),
            summary.get("overlay_cache_misses"),
        )
        summary["posting_base_decode_ns_per_member"] = safe_ratio(
            summary.get("posting_base_decode_ns"),
            summary.get("posting_base_decode_members"),
        )
        summary["posting_delta_replay_ns_per_record"] = safe_ratio(
            summary.get("posting_delta_replay_ns"),
            summary.get("posting_delta_replay_records"),
        )
        summary["query_materialization_ns_per_posting"] = safe_ratio(
            summary.get("overlay_ns"),
            summary.get("overlay_calls"),
        )
        summary["query_materialization_ns_per_member"] = safe_ratio(
            summary.get("overlay_ns"),
            summary.get("overlay_materialized_members"),
        )
    return summaries


def format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if is_number(value):
        number = float(value)
        if not math.isfinite(number):
            return ""
        if number == 0:
            return "0"
        if abs(number) >= 1000:
            return f"{number:.0f}"
        if abs(number) >= 10:
            return f"{number:.2f}"
        return f"{number:.4f}".rstrip("0").rstrip(".")
    return str(value)


def print_tsv(title: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    print(f"# {title}")
    print("\t".join(columns))
    for row in rows:
        print("\t".join(format_value(row.get(column)) for column in columns))


def print_markdown(title: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    print(f"## {title}")
    print("| " + " | ".join(columns) + " |")
    print("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        print("| " + " | ".join(format_value(row.get(column)) for column in columns) + " |")


def row_by_label(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("label")): row for row in rows if row.get("label") is not None}


def find_row(rows: list[dict[str, Any]], label: str, workload: str | None = None) -> dict[str, Any] | None:
    for row in rows:
        if row.get("label") != label:
            continue
        if workload is not None and row.get("workload") != workload:
            continue
        return row
    return None


def gate_check(checks: list[dict[str, Any]], name: str, passed: bool, detail: str, value: Any = None) -> None:
    checks.append(
        {
            "name": name,
            "passed": passed,
            "detail": detail,
            "value": value,
        }
    )


PRODUCTION_PROOF_MARKERS = [
    (
        "target_success",
        "spfresh-production-test success",
        "production proof must be captured from `zig build spfresh-production-test --summary all`",
    ),
    (
        "posting_recovery_artifact",
        "run test 5 pass (5 total)",
        "production proof must include the low-level posting-family recovery artifact",
    ),
    (
        "storage_crash_recovery",
        "base delta posting families survive modeled lsm crash after committed writes...OK",
        "production proof must include storage-backed modeled crash recovery",
    ),
    (
        "compact_tail_crash_recovery",
        "base delta compact grouped tail survives modeled lsm crash and folds...OK",
        "production proof must include compact grouped delta-tail crash recovery and foldability",
    ),
    (
        "direct_idle_runtime",
        "db runUntilIdle drains lazy dense posting maintenance...OK",
        "production proof must include the direct DB idle maintenance path",
    ),
    (
        "bounded_idle_convergence",
        "db runUntilIdle repeats bounded dense posting maintenance while debt remains...OK",
        "production proof must include repeated bounded idle maintenance while repair debt remains",
    ),
    (
        "resource_budget",
        "db dense posting maintenance respects resource manager budget...OK",
        "production proof must include ResourceManager budget rejection on direct maintenance",
    ),
    (
        "elapsed_budget",
        "db dense posting maintenance respects elapsed budget...OK",
        "production proof must include elapsed-budget stop behavior on direct maintenance",
    ),
    (
        "capacity_threshold_policy",
        "db dense posting maintenance thresholds clean at-capacity layout debt...OK",
        "production proof must include capacity-threshold maintenance scheduling",
    ),
    (
        "delta_tail_threshold_policy",
        "db dense posting maintenance thresholds and caps base delta tail folding...OK",
        "production proof must include thresholded and capped base-delta tail folding",
    ),
    (
        "bounded_capacity_debt_reporting",
        "posting maintenance result reports remaining capacity debt after bounded split budget...OK",
        "production proof must include bounded repair remaining-debt reporting",
    ),
    (
        "query_guardrail",
        "db dense posting maintenance query guardrail skips after slow profiled dense search...OK",
        "production proof must include query guardrail skip behavior",
    ),
    (
        "backend_runtime_manual",
        "db dense posting maintenance can submit through backend runtime...OK",
        "production proof must include manual BackendRuntime submission",
    ),
    (
        "backend_runtime_threaded",
        "db dense posting maintenance can submit through threaded backend runtime...OK",
        "production proof must include threaded BackendRuntime submission",
    ),
    (
        "backend_runtime_resource_budget",
        "db dense posting maintenance submitted through backend runtime respects resource manager budget...OK",
        "production proof must include BackendRuntime ResourceManager budget rejection",
    ),
    (
        "threaded_backend_runtime_resource_budget",
        "db dense posting maintenance submitted through threaded backend runtime respects resource manager budget...OK",
        "production proof must include threaded BackendRuntime ResourceManager budget rejection",
    ),
]


def add_production_proof_checks(checks: list[dict[str, Any]], proof_file: Path | None) -> None:
    if proof_file is None:
        gate_check(
            checks,
            "production_proof.present",
            False,
            "optimized gate requires --gate-production-proof-file from `zig build spfresh-production-test --summary all`",
        )
        return
    if not proof_file.exists():
        gate_check(
            checks,
            "production_proof.present",
            False,
            "optimized gate requires an existing production proof log",
            str(proof_file),
        )
        return
    content = proof_file.read_text(encoding="utf-8", errors="replace")
    gate_check(
        checks,
        "production_proof.present",
        True,
        "production proof log exists",
        str(proof_file),
    )
    for name, marker, detail in PRODUCTION_PROOF_MARKERS:
        gate_check(
            checks,
            f"production_proof.{name}",
            marker in content,
            detail,
            marker if marker in content else "missing",
        )


def numeric_at_most(value: Any, limit: float) -> bool:
    return is_number(value) and float(value) <= limit


def numeric_at_least(value: Any, limit: float) -> bool:
    return is_number(value) and float(value) >= limit


def numeric_above(value: Any, limit: float) -> bool:
    return is_number(value) and float(value) > limit


def max_numeric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if is_number(row.get(key))]
    if not values:
        return None
    return max(values)


def non_negative_difference(left: Any, right: Any) -> float | None:
    if not is_number(left) or not is_number(right):
        return None
    return max(float(left) - float(right), 0.0)


def row_models_db_backed_vectors(row: dict[str, Any]) -> bool:
    if row.get("skip_vector_store") is False:
        return True
    if row.get("skip_vector_store") is not True:
        return False
    if row.get("dataset_mode") != "procedural":
        return False
    workload = str(row.get("workload", ""))
    return workload in {
        "bulk_build_external_vectors_sequential_empty",
        "online_batches_dense_external_vectors_empty",
        "online_batches_dense_external_vectors_per_batch_session_empty",
        "overwrite_hot_vectors_warm",
        "overwrite_random_vectors_warm",
        "overwrite_semantic_drift_vectors_warm",
        "append_streaming_warm",
        "mixed_insert_delete_update_warm",
    }


def is_bulk_ingest_workload(row: dict[str, Any]) -> bool:
    return str(row.get("workload", "")) in {
        "bulk_build_external_vectors_sequential_empty",
        "batch_apply_dense_external_vectors_warm",
        "online_batches_dense_external_vectors_empty",
        "online_batches_dense_external_vectors_per_batch_session_empty",
    }


def choose_largest_existing_label(writes: dict[str, dict[str, Any]], labels: list[str]) -> str:
    best_label: str | None = None
    best_vectors = -1.0
    for label in labels:
        row = writes.get(label)
        if row is None:
            continue
        vectors = row.get("vectors")
        vector_count = float(vectors) if is_number(vectors) else 0.0
        if best_label is None or vector_count > best_vectors:
            best_label = label
            best_vectors = vector_count
    if best_label is not None:
        return best_label
    return labels[0]


def required_write_recall_total(args: argparse.Namespace, label: str) -> float:
    if label.startswith("write_vdbb_1m_"):
        return args.gate_min_vdbb_1m_recall_total
    return args.gate_min_post_write_recall_total


def required_write_vectors(args: argparse.Namespace, label: str) -> float:
    if label.startswith("write_vdbb_1m_"):
        return args.gate_min_vectors
    return args.gate_min_local_vectors


def choose_procedural_mutation_label(
    writes: dict[str, dict[str, Any]],
    local_labels: list[str],
    distribution: str,
    suffix: str | list[str],
) -> str:
    suffixes = [suffix] if isinstance(suffix, str) else suffix
    procedural_labels: list[str] = []
    for item in suffixes:
        procedural_labels.append(f"write_vdbb_procedural_{distribution}_{item}")
        procedural_labels.append(f"write_vdbb_1m_procedural_{distribution}_{item}")
    return choose_largest_existing_label(
        writes,
        local_labels + procedural_labels,
    )


def evaluate_optimized_gate(args: argparse.Namespace, write_rows: list[dict[str, Any]], read_rows: list[dict[str, Any]]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    add_production_proof_checks(checks, args.gate_production_proof_file)
    writes = row_by_label(write_rows)

    vdbb_baseline_label = choose_largest_existing_label(
        writes,
        [
            "write_vdbb_packed_hbc_hbc",
            "write_vdbb_1m_procedural_packed_hbc_hbc",
        ],
    )
    vdbb_quality_label = choose_largest_existing_label(
        writes,
        [
            "write_vdbb_1m_procedural_base_delta_hbc_bounded_maintenance",
            "write_vdbb_procedural_base_delta_two_level_rabitq_bounded_maintenance",
            "write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance",
            "write_vdbb_procedural_base_delta_hbc_bounded_maintenance",
            "write_vdbb_base_delta_hbc_reassign_capacity",
        ],
    )
    hot_baseline_label = choose_procedural_mutation_label(writes, ["write_packed_hbc_hbc"], "hot", "packed_hbc_hbc")
    random_baseline_label = choose_procedural_mutation_label(writes, ["write_random_packed_hbc_hbc"], "random", "packed_hbc_hbc")
    semantic_baseline_label = choose_procedural_mutation_label(writes, ["write_semantic_packed_hbc_hbc"], "semantic", "packed_hbc_hbc")
    append_baseline_label = choose_procedural_mutation_label(writes, ["write_append_packed_hbc_hbc"], "append", "packed_hbc_hbc")
    mixed_baseline_label = choose_procedural_mutation_label(writes, ["write_mixed_packed_hbc_hbc"], "mixed", "packed_hbc_hbc")

    hot_quality_label = choose_procedural_mutation_label(
        writes,
        [
            "write_base_delta_hbc_reassign_capacity",
            "write_base_delta_two_level_rabitq",
        ],
        "hot",
        [
            "base_delta_hbc_bounded_maintenance",
            "base_delta_two_level_rabitq_bounded_maintenance",
        ],
    )
    random_quality_label = choose_procedural_mutation_label(
        writes,
        [
            "write_random_base_delta_hbc_reassign_capacity",
            "write_random_base_delta_two_level_rabitq_reassign_capacity",
        ],
        "random",
        [
            "base_delta_hbc_bounded_maintenance",
            "base_delta_two_level_rabitq_bounded_maintenance",
        ],
    )
    semantic_quality_label = choose_procedural_mutation_label(
        writes,
        [
            "write_semantic_base_delta_hbc_reassign_capacity",
            "write_semantic_base_delta_two_level_rabitq_reassign_capacity",
        ],
        "semantic",
        [
            "base_delta_hbc_bounded_maintenance",
            "base_delta_two_level_rabitq_bounded_maintenance",
        ],
    )
    append_quality_label = choose_procedural_mutation_label(
        writes,
        [
            "write_append_base_delta_hbc_lazy_fold",
            "write_append_base_delta_two_level_rabitq_lazy_fold",
        ],
        "append",
        [
            "base_delta_hbc_bounded_maintenance",
            "base_delta_two_level_rabitq_bounded_maintenance",
        ],
    )
    mixed_quality_label = choose_procedural_mutation_label(
        writes,
        [
            "write_mixed_base_delta_hbc_reassign_capacity",
            "write_mixed_base_delta_two_level_rabitq_reassign_capacity",
        ],
        "mixed",
        [
            "base_delta_hbc_bounded_maintenance",
            "base_delta_two_level_rabitq_bounded_maintenance",
        ],
    )

    max_vectors = max_numeric(write_rows + read_rows, "vectors")
    gate_check(
        checks,
        "scale",
        max_vectors is not None and max_vectors >= args.gate_min_vectors,
        f"max active/query vectors must be >= {args.gate_min_vectors}",
        max_vectors,
    )

    required_write_baseline_labels = [
        hot_baseline_label,
        "write_same_leaf_packed_hbc_hbc",
        random_baseline_label,
        semantic_baseline_label,
        append_baseline_label,
        mixed_baseline_label,
        vdbb_baseline_label,
    ]
    for label in required_write_baseline_labels:
        row = writes.get(label)
        gate_check(checks, f"{label}.present", row is not None, "required packed-HBC write baseline row exists")
        if row is None:
            continue
        min_vectors = required_write_vectors(args, label)
        gate_check(
            checks,
            f"{label}.scale",
            numeric_at_least(row.get("vectors"), min_vectors),
            f"required packed-HBC write baseline active vectors must be >= {min_vectors}",
            row.get("vectors"),
        )
        if not args.gate_allow_skip_vector_store:
            gate_check(
                checks,
                f"{label}.vector_store_enabled",
                row_models_db_backed_vectors(row),
                "primary optimized gate requires in-DB vector storage or procedural DB-backed vector rows; use --gate-allow-skip-vector-store for unrelated external-vector experiments",
                f"skip={format_value(row.get('skip_vector_store'))}; mode={format_value(row.get('dataset_mode'))}",
            )
        for metric in ("recall", "qps", "p95_ns", "fg_node_bytes", "storage_write_bytes", "storage_write_files", "lsm_run_bytes", "lsm_runs"):
            metric_ok = numeric_at_least(row.get(metric), 0) if metric == "recall" else numeric_above(row.get(metric), 0)
            gate_check(
                checks,
                f"{label}.{metric}",
                metric_ok,
                f"required packed-HBC write baseline must report {metric}",
                row.get(metric),
            )
        gate_check(
            checks,
            f"{label}.exact_recall",
            row.get("recall_mode") == "exact",
            "optimized gate requires exact post-write recall, not self-hit validation",
            row.get("recall_mode"),
        )
        min_recall_total = required_write_recall_total(args, label)
        gate_check(
            checks,
            f"{label}.recall_total",
            numeric_at_least(row.get("recall_total"), min_recall_total),
            f"exact post-write recall denominator must be >= {min_recall_total}",
            row.get("recall_total"),
        )
        gate_check(
            checks,
            f"{label}.exact_truth_precomputed",
            numeric_at_least(row.get("post_write_exact_truth_build_ns"), 0),
            "required exact-recall write baseline must report precomputed/loadable post-write truth time outside query timing",
            row.get("post_write_exact_truth_build_ns"),
        )

    packed_read = find_row(read_rows, "read_packed_hbc_hbc", "warm_query_no_metadata")
    gate_check(checks, "read_packed_hbc_hbc.present", packed_read is not None, "required packed-HBC read baseline row exists")
    if packed_read is not None:
        gate_check(
            checks,
            "read_packed_hbc_hbc.scale",
            numeric_at_least(packed_read.get("vectors"), args.gate_min_local_vectors),
            f"required packed-HBC read baseline vectors must be >= {args.gate_min_local_vectors}",
            packed_read.get("vectors"),
        )
        for metric in ("recall", "qps", "p95_ns"):
            metric_ok = numeric_at_least(packed_read.get(metric), 0) if metric == "recall" else numeric_above(packed_read.get(metric), 0)
            gate_check(
                checks,
                f"read_packed_hbc_hbc.{metric}",
                metric_ok,
                f"required packed-HBC read baseline must report {metric}",
                packed_read.get(metric),
            )
        gate_check(
            checks,
            "read_packed_hbc_hbc.recall_total",
            numeric_at_least(packed_read.get("recall_total"), args.gate_min_read_recall_total),
            f"read-bench recall denominator must be >= {args.gate_min_read_recall_total}",
            packed_read.get("recall_total"),
        )

    quality_write_labels = [
        "write_same_leaf_base_delta_hbc",
        hot_quality_label,
        random_quality_label,
        semantic_quality_label,
        append_quality_label,
        mixed_quality_label,
        vdbb_quality_label,
    ]
    reassignment_budget_labels = {
        hot_quality_label,
        random_quality_label,
        semantic_quality_label,
        mixed_quality_label,
        vdbb_quality_label,
    }
    foreground_split_deferral_labels = {
        append_quality_label,
        mixed_quality_label,
        vdbb_quality_label,
    }
    for label in quality_write_labels:
        row = writes.get(label)
        gate_check(checks, f"{label}.present", row is not None, "required write comparison row exists")
        if row is None:
            continue
        min_vectors = required_write_vectors(args, label)
        gate_check(
            checks,
            f"{label}.scale",
            numeric_at_least(row.get("vectors"), min_vectors),
            f"required write row active vectors must be >= {min_vectors}",
            row.get("vectors"),
        )
        if not args.gate_allow_skip_vector_store:
            gate_check(
                checks,
                f"{label}.vector_store_enabled",
                row_models_db_backed_vectors(row),
                "primary optimized gate requires in-DB vector storage or procedural DB-backed vector rows; use --gate-allow-skip-vector-store for unrelated external-vector experiments",
                f"skip={format_value(row.get('skip_vector_store'))}; mode={format_value(row.get('dataset_mode'))}",
            )
        gate_check(
            checks,
            f"{label}.recall",
            numeric_at_least(row.get("recall_delta"), -args.gate_max_recall_drop),
            f"recall_delta must be >= -{args.gate_max_recall_drop}",
            row.get("recall_delta"),
        )
        gate_check(
            checks,
            f"{label}.exact_recall",
            row.get("recall_mode") == "exact",
            "optimized gate requires exact post-write recall, not self-hit validation",
            row.get("recall_mode"),
        )
        min_recall_total = required_write_recall_total(args, label)
        gate_check(
            checks,
            f"{label}.recall_total",
            numeric_at_least(row.get("recall_total"), min_recall_total),
            f"exact post-write recall denominator must be >= {min_recall_total}",
            row.get("recall_total"),
        )
        gate_check(
            checks,
            f"{label}.exact_truth_precomputed",
            numeric_at_least(row.get("post_write_exact_truth_build_ns"), 0),
            "optimized exact-recall write rows must report precomputed/loadable post-write truth time outside query timing",
            row.get("post_write_exact_truth_build_ns"),
        )
        gate_check(
            checks,
            f"{label}.qps",
            numeric_at_least(row.get("qps_vs_packed"), args.gate_min_qps_vs_packed),
            f"qps_vs_packed must be >= {args.gate_min_qps_vs_packed}",
            row.get("qps_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.p95_latency",
            numeric_at_most(row.get("p95_vs_packed"), args.gate_max_p95_vs_packed),
            f"p95_vs_packed must be <= {args.gate_max_p95_vs_packed}",
            row.get("p95_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.warm_internal_quantized_residency",
            numeric_at_most(row.get("warm_quant_internal_misses"), args.gate_max_warm_internal_quantized_misses),
            f"warm internal quantized misses must be <= {args.gate_max_warm_internal_quantized_misses}",
            row.get("warm_quant_internal_misses"),
        )
        if not is_bulk_ingest_workload(row):
            gate_check(
                checks,
                f"{label}.pre_repair_recall_present",
                is_number(row.get("pre_repair_recall")),
                "optimized non-bulk write rows must report pre-repair recall",
                row.get("pre_repair_recall"),
            )
            gate_check(
                checks,
                f"{label}.pre_repair_recall",
                numeric_at_least(row.get("pre_repair_recall_delta"), -args.gate_max_pre_repair_recall_drop),
                f"pre_repair_recall_delta must be >= -{args.gate_max_pre_repair_recall_drop}",
                row.get("pre_repair_recall_delta"),
            )
            gate_check(
                checks,
                f"{label}.pre_repair_qps",
                numeric_at_least(row.get("pre_repair_qps_vs_packed"), args.gate_min_pre_repair_qps_vs_packed),
                f"pre_repair_qps_vs_packed must be >= {args.gate_min_pre_repair_qps_vs_packed}",
                row.get("pre_repair_qps_vs_packed"),
            )
            gate_check(
                checks,
                f"{label}.pre_repair_p95_latency",
                numeric_at_most(row.get("pre_repair_p95_vs_packed"), args.gate_max_pre_repair_p95_vs_packed),
                f"pre_repair_p95_vs_packed must be <= {args.gate_max_pre_repair_p95_vs_packed}",
                row.get("pre_repair_p95_vs_packed"),
            )
        if row.get("workload") == "overwrite_same_leaf_vectors_warm":
            gate_check(
                checks,
                f"{label}.no_repair_split_scheduling",
                row.get("repair_split_full") is False,
                "same-leaf metadata-only optimized rows should not schedule posting splits",
                row.get("repair_split_full"),
            )
            gate_check(
                checks,
                f"{label}.no_repair_work",
                numeric_at_most(row.get("repair_split"), 0) and numeric_at_most(row.get("repair_reassign"), 0) and numeric_at_most(row.get("repair_fold_records"), 0),
                "same-leaf metadata-only optimized rows should not need split/reassign/fold repair",
                f"{format_value(row.get('repair_split'))}/{format_value(row.get('repair_reassign'))}/{format_value(row.get('repair_fold_records'))}",
            )
            gate_check(
                checks,
                f"{label}.warm_read_ranges",
                numeric_at_most(row.get("warm_query_read_ranges"), args.gate_max_same_leaf_warm_read_ranges),
                f"same-leaf warm post-write queries must perform <= {args.gate_max_same_leaf_warm_read_ranges} range reads",
                row.get("warm_query_read_ranges"),
            )
        else:
            gate_check(
                checks,
                f"{label}.split_scheduling",
                row.get("repair_rebalance_layout") is True or row.get("repair_split_full") is True,
                "optimized write rows must enable repair-side split scheduling",
                f"{format_value(row.get('repair_rebalance_layout'))}/{format_value(row.get('repair_split_full'))}",
            )
            gate_check(
                checks,
                f"{label}.layout_budget",
                numeric_at_least(row.get("repair_layout_limit"), 1),
                "optimized write rows must publish a finite repair layout-change budget",
                row.get("repair_layout_limit"),
            )
        if label in reassignment_budget_labels:
            gate_check(
                checks,
                f"{label}.repair_reassignment_budget",
                numeric_at_least(row.get("repair_reassign_budget"), 1),
                "capacity-reassignment optimized rows must publish a finite repair reassignment budget",
                row.get("repair_reassign_budget"),
            )
            gate_check(
                checks,
                f"{label}.repair_reassignment_limits",
                numeric_at_most(row.get("repair_overfull_limit"), 0)
                and numeric_at_most(row.get("repair_over_capacity_limit"), 0),
                "capacity-reassignment optimized rows must publish zero overfull/over-capacity limits when overfull moves are disabled",
                f"{format_value(row.get('repair_overfull_limit'))}/{format_value(row.get('repair_over_capacity_limit'))}",
            )
        gate_check(
            checks,
            f"{label}.repair_non_overfull_reassignment",
            row.get("repair_allow_overfull") is False,
            "optimized write rows must keep explicit repair overfull reassignment disabled",
            row.get("repair_allow_overfull"),
        )
        if label in foreground_split_deferral_labels:
            gate_check(
                checks,
                f"{label}.foreground_split_deferral",
                row.get("defer_leaf_splits_to_posting_maintenance") is True,
                "append/mixed optimized rows must defer oversized posting splits to posting maintenance",
                row.get("defer_leaf_splits_to_posting_maintenance"),
            )
        gate_check(
            checks,
            f"{label}.auto_non_overfull_reassignment",
            row.get("auto_allow_overfull") is False,
            "optimized write rows must keep automatic overfull reassignment disabled",
            row.get("auto_allow_overfull"),
        )
        gate_check(
            checks,
            f"{label}.auto_reassignment_limits",
            numeric_at_most(row.get("auto_overfull_limit"), 0)
            and numeric_at_most(row.get("auto_over_capacity_limit"), 0),
            "optimized write rows must publish zero automatic overfull/over-capacity limits when overfull moves are disabled",
            f"{format_value(row.get('auto_overfull_limit'))}/{format_value(row.get('auto_over_capacity_limit'))}",
        )
        gate_check(
            checks,
            f"{label}.overlay_cache_enabled",
            numeric_at_least(row.get("overlay_cache_bytes"), args.gate_min_overlay_cache_bytes),
            f"overlay cache bytes must be >= {args.gate_min_overlay_cache_bytes}",
            row.get("overlay_cache_bytes"),
        )
        gate_check(
            checks,
            f"{label}.overlay_cache_bounded",
            numeric_at_most(row.get("overlay_cache_bytes"), args.gate_max_overlay_cache_bytes),
            f"overlay cache bytes must be <= {args.gate_max_overlay_cache_bytes}",
            row.get("overlay_cache_bytes"),
        )
        gate_check(
            checks,
            f"{label}.overlay_cache_resident_bytes",
            numeric_at_most(row.get("overlay_cache_member_bytes"), float(row.get("overlay_cache_bytes")))
            if is_number(row.get("overlay_cache_bytes"))
            else False,
            "resident overlay cache member bytes must stay within configured cache bytes",
            row.get("overlay_cache_member_bytes"),
        )
        overlay_hits = row.get("overlay_cache_hits")
        overlay_misses = row.get("overlay_cache_misses")
        if is_number(overlay_hits) and is_number(overlay_misses) and float(overlay_hits) + float(overlay_misses) > 0:
            overlay_warm_hits = row.get("overlay_cache_warm_hits")
            overlay_warm_misses = row.get("overlay_cache_warm_misses")
            warm_overlay_observations = (
                float(overlay_warm_hits) + float(overlay_warm_misses)
                if is_number(overlay_warm_hits) and is_number(overlay_warm_misses)
                else 0.0
            )
            gate_check(
                checks,
                f"{label}.warm_overlay_cache_evidence",
                numeric_at_least(row.get("post_write_query_rounds"), 2)
                and numeric_at_least(row.get("post_write_warm_queries"), 1)
                and warm_overlay_observations > 0,
                "overlay cache hit-rate proof requires at least one warm post-write query round with overlay-cache observations",
                f"rounds={format_value(row.get('post_write_query_rounds'))}; warm_queries={format_value(row.get('post_write_warm_queries'))}; warm_observations={format_value(warm_overlay_observations)}",
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_hit_rate",
                numeric_at_least(row.get("overlay_cache_warm_hit_rate"), args.gate_min_write_overlay_cache_hit_rate),
                f"warm post-write overlay cache hit rate must be >= {args.gate_min_write_overlay_cache_hit_rate}",
                row.get("overlay_cache_warm_hit_rate"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_admission_skips",
                numeric_at_most(row.get("overlay_cache_admission_skips"), args.gate_max_overlay_cache_admission_skips),
                f"posting overlay cache admission skips must be <= {args.gate_max_overlay_cache_admission_skips}",
                row.get("overlay_cache_admission_skips"),
            )
        if is_bulk_ingest_workload(row):
            gate_check(
                checks,
                f"{label}.repair_before_bulk_finish",
                row.get("repair_before_bulk_finish") is True,
                "VDBB-shaped optimized rows must repair before bulk finish to keep maintenance in the bulk ingest window",
                row.get("repair_before_bulk_finish"),
            )
            gate_check(
                checks,
                f"{label}.foreground_profile",
                numeric_at_least(row.get("fg_save_nodes"), 0),
                "VDBB-shaped optimized rows must report foreground-only write profile counters separately from repair work",
                row.get("fg_save_nodes"),
            )

    auto_maintenance_labels = [
        "write_base_delta_hbc_auto_split_full",
    ]
    for label in auto_maintenance_labels:
        row = writes.get(label)
        gate_check(checks, f"{label}.present", row is not None, "required automatic-maintenance write comparison row exists")
        if row is None:
            continue
        min_vectors = required_write_vectors(args, label)
        gate_check(
            checks,
            f"{label}.scale",
            numeric_at_least(row.get("vectors"), min_vectors),
            f"required automatic-maintenance write row active vectors must be >= {min_vectors}",
            row.get("vectors"),
        )
        if not args.gate_allow_skip_vector_store:
            gate_check(
                checks,
                f"{label}.vector_store_enabled",
                row_models_db_backed_vectors(row),
                "primary optimized gate requires in-DB vector storage or procedural DB-backed vector rows; use --gate-allow-skip-vector-store for unrelated external-vector experiments",
                f"skip={format_value(row.get('skip_vector_store'))}; mode={format_value(row.get('dataset_mode'))}",
            )
        gate_check(
            checks,
            f"{label}.recall",
            numeric_at_least(row.get("recall_delta"), -args.gate_max_recall_drop),
            f"recall_delta must be >= -{args.gate_max_recall_drop}",
            row.get("recall_delta"),
        )
        gate_check(
            checks,
            f"{label}.exact_recall",
            row.get("recall_mode") == "exact",
            "optimized gate requires exact post-write recall, not self-hit validation",
            row.get("recall_mode"),
        )
        min_recall_total = required_write_recall_total(args, label)
        gate_check(
            checks,
            f"{label}.recall_total",
            numeric_at_least(row.get("recall_total"), min_recall_total),
            f"exact post-write recall denominator must be >= {min_recall_total}",
            row.get("recall_total"),
        )
        gate_check(
            checks,
            f"{label}.exact_truth_precomputed",
            numeric_at_least(row.get("post_write_exact_truth_build_ns"), 0),
            "automatic-maintenance exact-recall write rows must report precomputed/loadable post-write truth time outside query timing",
            row.get("post_write_exact_truth_build_ns"),
        )
        gate_check(
            checks,
            f"{label}.qps",
            numeric_at_least(row.get("qps_vs_packed"), args.gate_min_qps_vs_packed),
            f"qps_vs_packed must be >= {args.gate_min_qps_vs_packed}",
            row.get("qps_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.p95_latency",
            numeric_at_most(row.get("p95_vs_packed"), args.gate_max_p95_vs_packed),
            f"p95_vs_packed must be <= {args.gate_max_p95_vs_packed}",
            row.get("p95_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.warm_internal_quantized_residency",
            numeric_at_most(row.get("warm_quant_internal_misses"), args.gate_max_warm_internal_quantized_misses),
            f"warm internal quantized misses must be <= {args.gate_max_warm_internal_quantized_misses}",
            row.get("warm_quant_internal_misses"),
        )
        gate_check(
            checks,
            f"{label}.bounded_auto_postings",
            numeric_at_least(row.get("auto_max_postings"), 1)
            and numeric_at_most(row.get("auto_max_postings"), args.gate_max_auto_maintenance_postings),
            f"auto maintenance max postings must be in [1, {args.gate_max_auto_maintenance_postings}]",
            row.get("auto_max_postings"),
        )
        gate_check(
            checks,
            f"{label}.bounded_auto_layout",
            numeric_at_least(row.get("auto_layout_limit"), 1),
            "auto maintenance must publish a finite layout-change budget",
            row.get("auto_layout_limit"),
        )
        gate_check(
            checks,
            f"{label}.bounded_auto_delta_tail",
            numeric_at_most(row.get("auto_delta_tail_limit"), args.gate_max_auto_delta_tail_postings),
            f"auto maintenance max delta-tail postings must be <= {args.gate_max_auto_delta_tail_postings}",
            row.get("auto_delta_tail_limit"),
        )
        gate_check(
            checks,
            f"{label}.thresholded_auto_fold",
            row.get("auto_fold_delta_tails") is True
            and numeric_at_least(row.get("auto_min_delta_records_to_fold"), args.gate_min_auto_delta_records_to_fold)
            and numeric_at_least(row.get("auto_min_tombstone_records_to_fold"), args.gate_min_auto_tombstone_records_to_fold),
            "auto maintenance must keep fold thresholds above eager values",
            f"{format_value(row.get('auto_min_delta_records_to_fold'))}/{format_value(row.get('auto_min_tombstone_records_to_fold'))}",
        )
        gate_check(
            checks,
            f"{label}.auto_split_full_policy",
            row.get("auto_split_full") is True and numeric_at_least(row.get("auto_min_capacity_to_run"), 1),
            "auto maintenance must schedule full-posting layout work from capacity debt",
            f"{format_value(row.get('auto_split_full'))}/{format_value(row.get('auto_min_capacity_to_run'))}",
        )
        gate_check(
            checks,
            f"{label}.auto_boundary_reassignment_budget",
            numeric_at_least(row.get("auto_boundary_reassignments"), 1),
            "auto maintenance must publish a finite boundary-reassignment budget",
            row.get("auto_boundary_reassignments"),
        )
        gate_check(
            checks,
            f"{label}.auto_non_overfull_reassignment",
            row.get("auto_allow_overfull") is False,
            "automatic maintenance must keep overfull reassignment disabled",
            row.get("auto_allow_overfull"),
        )
        gate_check(
            checks,
            f"{label}.auto_reassignment_limits",
            numeric_at_most(row.get("auto_overfull_limit"), 0)
            and numeric_at_most(row.get("auto_over_capacity_limit"), 0),
            "automatic maintenance must publish zero overfull/over-capacity limits when overfull moves are disabled",
            f"{format_value(row.get('auto_overfull_limit'))}/{format_value(row.get('auto_over_capacity_limit'))}",
        )
        gate_check(
            checks,
            f"{label}.auto_maintenance_ran",
            numeric_at_least(row.get("auto_runs"), 1) and numeric_at_least(row.get("maintenance_repaired"), 1),
            "automatic-maintenance row must perform bounded repair work during the write phase",
            f"{format_value(row.get('auto_runs'))}/{format_value(row.get('maintenance_repaired'))}",
        )
        gate_check(
            checks,
            f"{label}.auto_layout_work_bounded",
            numeric_at_least(row.get("maintenance_split"), 1)
            and numeric_at_most(row.get("auto_max_layout_observed"), float(row.get("auto_layout_limit")))
            if is_number(row.get("auto_layout_limit"))
            else False,
            "scheduled automatic split work must run and each pass must stay within the layout budget",
            f"{format_value(row.get('maintenance_split'))}; max/pass={format_value(row.get('auto_max_layout_observed'))}/{format_value(row.get('auto_layout_limit'))}",
        )

    write_amp_labels = [
        "write_same_leaf_base_delta_hbc",
        hot_quality_label,
        random_quality_label,
        semantic_quality_label,
        append_quality_label,
        mixed_quality_label,
        "write_base_delta_hbc_auto_split_full",
    ]
    for label in write_amp_labels:
        row = writes.get(label)
        if row is None:
            continue
        split_value = row.get("fg_non_maintenance_splits") if label in auto_maintenance_labels else row.get("fg_splits")
        split_detail = (
            f"non-maintenance foreground splits must be <= {args.gate_max_foreground_splits}"
            if label in auto_maintenance_labels
            else f"fg_splits must be <= {args.gate_max_foreground_splits}"
        )
        gate_check(
            checks,
            f"{label}.foreground_write_amp",
            numeric_at_most(row.get("fg_node_bytes_vs_packed"), args.gate_max_fg_node_bytes_vs_packed),
            f"fg_node_bytes_vs_packed must be <= {args.gate_max_fg_node_bytes_vs_packed}",
            row.get("fg_node_bytes_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.foreground_splits",
            numeric_at_most(split_value, args.gate_max_foreground_splits),
            split_detail,
            split_value,
        )
        gate_check(
            checks,
            f"{label}.storage_write_bytes",
            numeric_at_most(row.get("storage_write_bytes_vs_packed"), args.gate_max_storage_write_bytes_vs_packed),
            f"storage_write_bytes_vs_packed must be <= {args.gate_max_storage_write_bytes_vs_packed}",
            row.get("storage_write_bytes_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.logical_write_puts",
            True
            if label in auto_maintenance_labels
            else numeric_at_most(row.get("logical_write_puts_vs_packed"), args.gate_max_logical_write_puts_vs_packed),
            f"diagnostic for automatic-maintenance proof rows; optimized overwrite rows must be <= {args.gate_max_logical_write_puts_vs_packed}"
            if label in auto_maintenance_labels
            else f"logical_write_puts_vs_packed must be <= {args.gate_max_logical_write_puts_vs_packed}",
            row.get("logical_write_puts_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.lsm_run_bytes",
            numeric_at_most(row.get("lsm_run_bytes_vs_packed"), args.gate_max_lsm_run_bytes_vs_packed),
            f"lsm_run_bytes_vs_packed must be <= {args.gate_max_lsm_run_bytes_vs_packed}",
            row.get("lsm_run_bytes_vs_packed"),
        )

    bulk_ingest_storage_labels = [
        vdbb_quality_label,
    ]
    for label in bulk_ingest_storage_labels:
        row = writes.get(label)
        if row is None or not is_bulk_ingest_workload(row):
            continue
        gate_check(
            checks,
            f"{label}.bulk_storage_write_bytes",
            numeric_at_most(row.get("storage_write_bytes_vs_packed"), args.gate_max_storage_write_bytes_vs_packed),
            f"VDBB-shaped bulk-ingest storage_write_bytes_vs_packed must be <= {args.gate_max_storage_write_bytes_vs_packed}",
            row.get("storage_write_bytes_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.bulk_storage_write_files",
            True,
            f"diagnostic: VDBB-shaped bulk-ingest storage_write_files_vs_packed observed; overwrite rows enforce <= {args.gate_max_storage_write_files_vs_packed}",
            row.get("storage_write_files_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.bulk_lsm_run_bytes",
            numeric_at_most(row.get("lsm_run_bytes_vs_packed"), args.gate_max_lsm_run_bytes_vs_packed),
            f"VDBB-shaped bulk-ingest lsm_run_bytes_vs_packed must be <= {args.gate_max_lsm_run_bytes_vs_packed}",
            row.get("lsm_run_bytes_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.bulk_lsm_runs",
            True,
            f"diagnostic: VDBB-shaped bulk-ingest lsm_l0_runs_vs_packed observed; overwrite rows enforce <= {args.gate_max_lsm_runs_vs_packed}",
            row.get("lsm_l0_runs_vs_packed"),
        )
        gate_check(
            checks,
            f"{label}.bulk_derived_assignment_map",
            numeric_at_most(row.get("assignment_map_put_calls"), 0),
            "VDBB-shaped canonical base-delta bulk rows must derive assignment records from vec-leaf mappings instead of writing duplicate current assignment-map keys",
            row.get("assignment_map_put_calls"),
        )
        if row.get("directory") == "two_level_rabitq":
            flat_probe_count = row.get("flat_probe_count")
            block_probe_count = effective_two_level_block_probe_budget(row)
            gate_check(
                checks,
                f"{label}.two_level_block_probe_budget",
                numeric_at_least(block_probe_count, flat_probe_count),
                "VDBB-shaped two-level rows must effectively probe at least as many centroid blocks as final posting probes to avoid over-pruning coarse blocks",
                f"effective={format_value(block_probe_count)} configured={format_value(row.get('flat_block_probe_count'))} final={format_value(flat_probe_count)}",
            )

    required_debt_labels = set(quality_write_labels + auto_maintenance_labels)
    grouped_delta_labels = {
        append_quality_label,
        mixed_quality_label,
        vdbb_quality_label,
    }
    for label, row in writes.items():
        if row.get("posting") != "base_delta":
            continue
        if row.get("repair_allow_overfull") is True:
            gate_check(
                checks,
                f"{label}.overfull_bounded",
                row.get("repair_overfull_limit") != "unbounded" and row.get("repair_over_capacity_limit") != "unbounded",
                "rows that enable overfull reassignment must publish explicit finite debt limits",
                f"{format_value(row.get('repair_overfull_limit'))}/{format_value(row.get('repair_over_capacity_limit'))}",
            )
        if label not in required_debt_labels:
            continue
        deferred_append_debt = label == append_quality_label
        dirty_debt_within_cap = (
            numeric_at_most(row.get("backlog_dirty"), args.gate_max_deferred_append_posting_debt)
            if deferred_append_debt
            else numeric_at_most(row.get("backlog_dirty"), args.gate_max_backlog_dirty)
        )
        overfull_debt_within_cap = (
            numeric_at_most(row.get("backlog_overfull"), args.gate_max_deferred_append_posting_debt)
            if deferred_append_debt
            else numeric_at_most(row.get("backlog_overfull"), args.gate_max_backlog_overfull)
        )
        over_capacity_debt_within_cap = (
            numeric_at_most(row.get("backlog_max_over_capacity"), args.gate_max_deferred_append_over_capacity_members)
            if deferred_append_debt
            else numeric_at_most(row.get("backlog_max_over_capacity"), args.gate_max_backlog_over_capacity_members)
        )
        delta_tail_debt_within_cap = (
            numeric_at_most(row.get("backlog_delta_tails"), args.gate_max_deferred_append_posting_debt)
            if deferred_append_debt
            else numeric_at_most(row.get("backlog_delta_tails"), args.gate_max_backlog_delta_tails)
        )
        full_posting_debt_required = row.get("repair_split_full") is True or row.get("auto_split_full") is True
        full_posting_debt_within_cap = (
            numeric_at_most(row.get("backlog_at_capacity"), args.gate_max_backlog_at_capacity)
            if full_posting_debt_required
            else True
        )
        debt_within_caps = (
            dirty_debt_within_cap
            and overfull_debt_within_cap
            and over_capacity_debt_within_cap
            and full_posting_debt_within_cap
            and delta_tail_debt_within_cap
        )
        gate_check(
            checks,
            f"{label}.repair_limit",
            row.get("repair_limit_reached") is not True or debt_within_caps,
            "bounded repair pass must not leave residual debt beyond optimized backlog caps",
            f"limit={format_value(row.get('repair_limit_reached'))}; dirty={format_value(row.get('backlog_dirty'))}; delta_tails={format_value(row.get('backlog_delta_tails'))}; overfull={format_value(row.get('backlog_overfull'))}; at_capacity={format_value(row.get('backlog_at_capacity'))}; max_over_capacity={format_value(row.get('backlog_max_over_capacity'))}",
        )
        gate_check(
            checks,
            f"{label}.dirty_debt",
            dirty_debt_within_cap,
            f"backlog_dirty must be <= {args.gate_max_deferred_append_posting_debt if deferred_append_debt else args.gate_max_backlog_dirty}",
            f"{format_value(row.get('backlog_dirty'))}; deferred_append={format_value(deferred_append_debt)}",
        )
        gate_check(
            checks,
            f"{label}.overfull_debt",
            overfull_debt_within_cap,
            f"backlog_overfull must be <= {args.gate_max_deferred_append_posting_debt if deferred_append_debt else args.gate_max_backlog_overfull}",
            f"{format_value(row.get('backlog_overfull'))}; deferred_append={format_value(deferred_append_debt)}",
        )
        gate_check(
            checks,
            f"{label}.over_capacity_debt",
            over_capacity_debt_within_cap,
            f"backlog_max_over_capacity must be <= {args.gate_max_deferred_append_over_capacity_members if deferred_append_debt else args.gate_max_backlog_over_capacity_members}",
            f"{format_value(row.get('backlog_max_over_capacity'))}; deferred_append={format_value(deferred_append_debt)}",
        )
        gate_check(
            checks,
            f"{label}.full_posting_debt",
            full_posting_debt_within_cap,
            f"backlog_at_capacity must be <= {args.gate_max_backlog_at_capacity} when full-posting split drain is enabled",
            f"{format_value(row.get('backlog_at_capacity'))}; split_full={format_value(row.get('repair_split_full'))}; auto_split_full={format_value(row.get('auto_split_full'))}",
        )
        gate_check(
            checks,
            f"{label}.delta_tail_debt",
            delta_tail_debt_within_cap,
            f"backlog_delta_tails must be <= {args.gate_max_deferred_append_posting_debt if deferred_append_debt else args.gate_max_backlog_delta_tails}",
            f"{format_value(row.get('backlog_delta_tails'))}; deferred_append={format_value(deferred_append_debt)}",
        )
        if is_number(row.get("fg_delta_records")) and float(row["fg_delta_records"]) > 0:
            gate_check(
                checks,
                f"{label}.delta_append_density",
                numeric_at_least(row.get("fg_delta_records_per_append"), args.gate_min_delta_records_per_append),
                f"foreground delta records per append must be >= {args.gate_min_delta_records_per_append}",
                row.get("fg_delta_records_per_append"),
            )
            if label in grouped_delta_labels:
                grouping_opportunity = numeric_at_least(row.get("fg_route_items_per_leaf_group"), args.gate_min_grouped_delta_records_per_append)
                gate_check(
                    checks,
                    f"{label}.grouped_delta_append_density",
                    (not grouping_opportunity)
                    or numeric_at_least(row.get("fg_delta_records_per_append"), args.gate_min_grouped_delta_records_per_append),
                    f"grouped foreground delta records per append must be >= {args.gate_min_grouped_delta_records_per_append} when average routed items per leaf group exposes grouping opportunity",
                    f"records_per_append={format_value(row.get('fg_delta_records_per_append'))}; route_items_per_group={format_value(row.get('fg_route_items_per_leaf_group'))}",
                )
                if numeric_at_least(row.get("fg_delta_records_per_append"), 3):
                    gate_check(
                        checks,
                        f"{label}.compact_delta_value_bytes",
                        numeric_at_most(row.get("fg_delta_value_bytes_vs_legacy"), args.gate_max_grouped_delta_value_bytes_vs_legacy),
                        f"grouped foreground delta value bytes / legacy v1 value bytes must be <= {args.gate_max_grouped_delta_value_bytes_vs_legacy} when appends average at least 3 records",
                        f"{format_value(row.get('fg_delta_value_bytes_vs_legacy'))} bytes_per_record={format_value(row.get('fg_delta_value_bytes_per_record'))}",
                    )
        if is_number(row.get("repair_fold_records")) and float(row["repair_fold_records"]) > 0:
            gate_check(
                checks,
                f"{label}.fold_tail_cleanup_keys",
                numeric_at_least(row.get("repair_fold_tail_keys"), 1),
                "rows that fold delta records must delete at least one physical tail key",
                row.get("repair_fold_tail_keys"),
            )
            gate_check(
                checks,
                f"{label}.fold_tail_cleanup_bytes",
                numeric_at_least(row.get("repair_fold_tail_value_bytes"), 1),
                "rows that fold delta records must delete physical tail value bytes",
                row.get("repair_fold_tail_value_bytes"),
            )
            gate_check(
                checks,
                f"{label}.fold_written_base_bytes",
                numeric_at_least(row.get("repair_fold_written_base_value_bytes"), 1),
                "rows that fold delta records must report replacement base value bytes",
                row.get("repair_fold_written_base_value_bytes"),
            )
            gate_check(
                checks,
                f"{label}.fold_base_bytes_per_record",
                numeric_at_most(row.get("repair_fold_base_bytes_per_record"), args.gate_max_fold_base_bytes_per_record),
                f"replacement base bytes per folded delta record must be <= {args.gate_max_fold_base_bytes_per_record}",
                row.get("repair_fold_base_bytes_per_record"),
            )
            gate_check(
                checks,
                f"{label}.fold_base_tail_ratio",
                numeric_at_most(row.get("repair_fold_base_tail_ratio"), args.gate_max_fold_base_tail_ratio),
                f"replacement base bytes / deleted tail value bytes must be <= {args.gate_max_fold_base_tail_ratio}",
                row.get("repair_fold_base_tail_ratio"),
            )

    flat_write_label = choose_largest_existing_label(
        writes,
        [
            "write_base_delta_flat_rabitq",
            "write_vdbb_procedural_base_delta_flat_rabitq",
            "write_vdbb_1m_procedural_base_delta_flat_rabitq",
        ],
    )
    two_level_write_label = choose_largest_existing_label(
        writes,
        [
            "write_base_delta_two_level_rabitq",
            "write_vdbb_procedural_base_delta_two_level_rabitq_bounded_maintenance",
            "write_vdbb_procedural_base_delta_two_level_rabitq",
            "write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance",
            "write_vdbb_1m_procedural_base_delta_two_level_rabitq",
        ],
    )
    flat_write = writes.get(flat_write_label)
    two_level_write = writes.get(two_level_write_label)
    gate_check(checks, "write_flat_rabitq.present", flat_write is not None, "flat write comparison row exists")
    gate_check(checks, "write_two_level_rabitq.present", two_level_write is not None, "two-level write comparison row exists")
    if flat_write is not None and two_level_write is not None:
        min_flat_vectors = required_write_vectors(args, flat_write_label)
        min_two_level_vectors = required_write_vectors(args, two_level_write_label)
        gate_check(
            checks,
            "write_flat_rabitq.scale",
            numeric_at_least(flat_write.get("vectors"), min_flat_vectors),
            f"required flat write row active vectors must be >= {min_flat_vectors}",
            f"{flat_write_label}: {format_value(flat_write.get('vectors'))}",
        )
        gate_check(
            checks,
            "write_two_level_rabitq.scale",
            numeric_at_least(two_level_write.get("vectors"), min_two_level_vectors),
            f"required two-level write row active vectors must be >= {min_two_level_vectors}",
            f"{two_level_write_label}: {format_value(two_level_write.get('vectors'))}",
        )
        if not args.gate_allow_skip_vector_store:
            gate_check(
                checks,
                "write_two_level_rabitq.vector_store_enabled",
                row_models_db_backed_vectors(two_level_write),
                "two-level write row must use in-DB vector storage or procedural DB-backed vector rows",
                f"{two_level_write_label}: skip={format_value(two_level_write.get('skip_vector_store'))}; mode={format_value(two_level_write.get('dataset_mode'))}",
            )
        gate_check(
            checks,
            "write_two_level_rabitq.recall",
            numeric_at_least(two_level_write.get("recall_delta"), -args.gate_max_recall_drop),
            f"selected two-level write row recall_delta must be >= -{args.gate_max_recall_drop}",
            f"{two_level_write_label}: {format_value(two_level_write.get('recall_delta'))}",
        )
        gate_check(
            checks,
            "write_two_level_rabitq.exact_recall",
            two_level_write.get("recall_mode") == "exact",
            "selected two-level write row must use exact post-write recall, not self-hit validation",
            f"{two_level_write_label}: {format_value(two_level_write.get('recall_mode'))}",
        )
        min_two_level_recall_total = required_write_recall_total(args, two_level_write_label)
        gate_check(
            checks,
            "write_two_level_rabitq.recall_total",
            numeric_at_least(two_level_write.get("recall_total"), min_two_level_recall_total),
            f"selected two-level exact post-write recall denominator must be >= {min_two_level_recall_total}",
            f"{two_level_write_label}: {format_value(two_level_write.get('recall_total'))}",
        )
        gate_check(
            checks,
            "write_two_level_rabitq.exact_truth_precomputed",
            numeric_at_least(two_level_write.get("post_write_exact_truth_build_ns"), 0),
            "selected two-level write row must report precomputed/loadable post-write truth time outside query timing",
            f"{two_level_write_label}: {format_value(two_level_write.get('post_write_exact_truth_build_ns'))}",
        )
        gate_check(
            checks,
            "write_two_level_rabitq.bounded_maintenance",
            "bounded_maintenance" in two_level_write_label,
            "selected two-level write row must be the bounded-maintenance variant",
            two_level_write_label,
        )
        gate_check(
            checks,
            "write_two_level_rabitq.qps",
            numeric_at_least(two_level_write.get("qps_vs_packed"), args.gate_min_qps_vs_packed),
            f"selected two-level write row qps_vs_packed must be >= {args.gate_min_qps_vs_packed}",
            f"{two_level_write_label}: {format_value(two_level_write.get('qps_vs_packed'))}",
        )
        gate_check(
            checks,
            "write_two_level_rabitq.p95_latency",
            numeric_at_most(two_level_write.get("p95_vs_packed"), args.gate_max_p95_vs_packed),
            f"selected two-level write row p95_vs_packed must be <= {args.gate_max_p95_vs_packed}",
            f"{two_level_write_label}: {format_value(two_level_write.get('p95_vs_packed'))}",
        )
        flat_write_posting_evaluations = numeric_sum(flat_write, "post_posting_centroids_scored", "post_posting_centroid_estimates")
        two_level_write_posting_evaluations = numeric_sum(two_level_write, "post_posting_centroids_scored", "post_posting_centroid_estimates")
        two_level_write_block_evaluations = numeric_sum(two_level_write, "post_block_centroids_scored", "post_block_centroid_estimates")
        write_selected = two_level_write.get("post_centroid_blocks_selected")
        write_scanned = two_level_write.get("post_centroid_blocks_scanned")
        write_block_pruning_required = numeric_at_least(write_scanned, args.gate_min_two_level_centroid_blocks)
        if is_number(flat_write_posting_evaluations) and is_number(two_level_write_posting_evaluations) and float(flat_write_posting_evaluations) > 0:
            write_pruning_ratio = float(two_level_write_posting_evaluations) / float(flat_write_posting_evaluations)
        else:
            write_pruning_ratio = None
        gate_check(
            checks,
            "write_two_level_rabitq.posting_centroid_pruning",
            (not write_block_pruning_required)
            or (write_pruning_ratio is not None and write_pruning_ratio <= args.gate_max_two_level_posting_centroid_ratio),
            f"write two_level posting centroid evaluations / flat evaluations must be <= {args.gate_max_two_level_posting_centroid_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            write_pruning_ratio,
        )
        write_block_scoring_ratio = None
        if is_number(flat_write_posting_evaluations) and is_number(two_level_write_block_evaluations) and float(flat_write_posting_evaluations) > 0:
            write_block_scoring_ratio = float(two_level_write_block_evaluations) / float(flat_write_posting_evaluations)
        gate_check(
            checks,
            "write_two_level_rabitq.coarse_centroid_cost",
            (not write_block_pruning_required)
            or (write_block_scoring_ratio is not None and write_block_scoring_ratio <= args.gate_max_two_level_block_centroid_ratio),
            f"write two_level block centroid evaluations / flat posting centroid evaluations must be <= {args.gate_max_two_level_block_centroid_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            write_block_scoring_ratio,
        )
        gate_check(
            checks,
            "write_two_level_rabitq.approximate_block_directory",
            numeric_at_least(two_level_write.get("post_block_centroid_estimates"), 1),
            "write two_level directory must estimate coarse block centroids through RaBitQ",
            two_level_write.get("post_block_centroid_estimates"),
        )
        write_selected_ratio = None
        if is_number(write_selected) and is_number(write_scanned) and float(write_scanned) > 0:
            write_selected_ratio = float(write_selected) / float(write_scanned)
        gate_check(
            checks,
            "write_two_level_rabitq.block_pruning",
            (not write_block_pruning_required)
            or (write_selected_ratio is not None and write_selected_ratio <= args.gate_max_two_level_block_select_ratio),
            f"write selected/scanned centroid blocks must be <= {args.gate_max_two_level_block_select_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            write_selected_ratio,
        )
        gate_check(
            checks,
            "write_two_level_rabitq.block_index_active",
            (not write_block_pruning_required)
            or (is_number(write_selected) and float(write_selected) < float(write_scanned)),
            f"write two_level directory must select fewer than scanned blocks once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            f"{format_value(write_selected)}/{format_value(write_scanned)}",
        )
    for label, row in (
        ("write_flat_rabitq", flat_write),
        ("write_two_level_rabitq", two_level_write),
    ):
        if row is None:
            continue
        gate_check(
            checks,
            f"{label}.record_directory_skips_hbc_pinning",
            numeric_at_most(row.get("post_upper_tree_pin_ns"), args.gate_max_directory_upper_tree_pin_ns),
            f"record-backed directory post-write queries must keep upper-tree pin time <= {args.gate_max_directory_upper_tree_pin_ns}",
            row.get("post_upper_tree_pin_ns"),
        )

    flat_read = find_row(read_rows, "read_base_delta_flat_rabitq_repaired", "warm_query_no_metadata")
    two_level_read = find_row(read_rows, "read_base_delta_two_level_rabitq_repaired", "warm_query_no_metadata")
    gate_check(checks, "read_flat_rabitq.present", flat_read is not None, "flat read comparison row exists")
    gate_check(checks, "read_two_level_rabitq.present", two_level_read is not None, "two-level read comparison row exists")
    if flat_read is not None and two_level_read is not None:
        for label, row in (
            ("read_flat_rabitq", flat_read),
            ("read_two_level_rabitq", two_level_read),
        ):
            gate_check(
                checks,
                f"{label}.scale",
                numeric_at_least(row.get("vectors"), args.gate_min_local_vectors),
                f"required read row vectors must be >= {args.gate_min_local_vectors}",
                row.get("vectors"),
            )
            gate_check(
                checks,
                f"{label}.recall",
                numeric_at_least(row.get("recall_delta"), -args.gate_max_read_recall_drop),
                f"read recall_delta must be >= -{args.gate_max_read_recall_drop}",
                row.get("recall_delta"),
            )
            gate_check(
                checks,
                f"{label}.recall_total",
                numeric_at_least(row.get("recall_total"), args.gate_min_read_recall_total),
                f"read-bench recall denominator must be >= {args.gate_min_read_recall_total}",
                row.get("recall_total"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_enabled",
                numeric_at_least(row.get("overlay_cache_bytes"), args.gate_min_overlay_cache_bytes),
                f"overlay cache bytes must be >= {args.gate_min_overlay_cache_bytes}",
                row.get("overlay_cache_bytes"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_bounded",
                numeric_at_most(row.get("overlay_cache_bytes"), args.gate_max_overlay_cache_bytes),
                f"overlay cache bytes must be <= {args.gate_max_overlay_cache_bytes}",
                row.get("overlay_cache_bytes"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_hit_rate",
                numeric_at_least(row.get("overlay_cache_hit_rate"), args.gate_min_read_overlay_cache_hit_rate),
                f"warm read overlay cache hit rate must be >= {args.gate_min_read_overlay_cache_hit_rate}",
                row.get("overlay_cache_hit_rate"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_admission_skips",
                numeric_at_most(row.get("overlay_cache_admission_skips"), args.gate_max_overlay_cache_admission_skips),
                f"posting overlay cache admission skips must be <= {args.gate_max_overlay_cache_admission_skips}",
                row.get("overlay_cache_admission_skips"),
            )
            gate_check(
                checks,
                f"{label}.overlay_cache_resident_bytes",
                numeric_at_most(row.get("overlay_cache_member_bytes"), float(row.get("overlay_cache_bytes")))
                if is_number(row.get("overlay_cache_bytes"))
                else False,
                "resident overlay cache member bytes must stay within configured cache bytes",
                row.get("overlay_cache_member_bytes"),
            )
            gate_check(
                checks,
                f"{label}.p95_latency",
                numeric_at_most(row.get("p95_vs_packed"), args.gate_max_read_p95_vs_packed),
                f"read p95_vs_packed must be <= {args.gate_max_read_p95_vs_packed}",
                row.get("p95_vs_packed"),
            )
        flat_posting_evaluations = numeric_sum(flat_read, "posting_centroids_scored", "posting_centroid_estimates")
        two_level_posting_evaluations = numeric_sum(two_level_read, "posting_centroids_scored", "posting_centroid_estimates")
        two_level_block_evaluations = numeric_sum(two_level_read, "block_centroids_scored", "block_centroid_estimates")
        selected = two_level_read.get("centroid_blocks_selected")
        scanned = two_level_read.get("centroid_blocks_scanned")
        flat_workspace = flat_read.get("workspace_bytes")
        two_level_workspace = two_level_read.get("workspace_bytes")
        block_pruning_required = numeric_at_least(scanned, args.gate_min_two_level_centroid_blocks)
        if is_number(flat_posting_evaluations) and is_number(two_level_posting_evaluations) and float(flat_posting_evaluations) > 0:
            pruning_ratio = float(two_level_posting_evaluations) / float(flat_posting_evaluations)
        else:
            pruning_ratio = None
        gate_check(
            checks,
            "two_level_rabitq.posting_centroid_pruning",
            (not block_pruning_required)
            or (pruning_ratio is not None and pruning_ratio <= args.gate_max_two_level_posting_centroid_ratio),
            f"two_level posting centroid evaluations / flat evaluations must be <= {args.gate_max_two_level_posting_centroid_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            pruning_ratio,
        )
        block_scoring_ratio = None
        if is_number(flat_posting_evaluations) and is_number(two_level_block_evaluations) and float(flat_posting_evaluations) > 0:
            block_scoring_ratio = float(two_level_block_evaluations) / float(flat_posting_evaluations)
        gate_check(
            checks,
            "two_level_rabitq.coarse_centroid_cost",
            (not block_pruning_required)
            or (block_scoring_ratio is not None and block_scoring_ratio <= args.gate_max_two_level_block_centroid_ratio),
            f"two_level block centroid evaluations / flat posting centroid evaluations must be <= {args.gate_max_two_level_block_centroid_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            block_scoring_ratio,
        )
        gate_check(
            checks,
            "two_level_rabitq.approximate_block_directory",
            numeric_at_least(two_level_read.get("block_centroid_estimates"), 1),
            "two_level directory must estimate coarse block centroids through RaBitQ",
            two_level_read.get("block_centroid_estimates"),
        )
        selected_ratio = None
        if is_number(selected) and is_number(scanned) and float(scanned) > 0:
            selected_ratio = float(selected) / float(scanned)
        gate_check(
            checks,
            "two_level_rabitq.block_pruning",
            (not block_pruning_required)
            or (selected_ratio is not None and selected_ratio <= args.gate_max_two_level_block_select_ratio),
            f"selected/scanned centroid blocks must be <= {args.gate_max_two_level_block_select_ratio} once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            selected_ratio,
        )
        gate_check(
            checks,
            "two_level_rabitq.block_index_active",
            (not block_pruning_required)
            or (is_number(selected) and float(selected) < float(scanned)),
            f"two_level directory must select fewer than scanned blocks once scanned blocks >= {args.gate_min_two_level_centroid_blocks}",
            f"{format_value(selected)}/{format_value(scanned)}",
        )
        workspace_ratio = None
        if is_number(flat_workspace) and is_number(two_level_workspace) and float(flat_workspace) > 0:
            workspace_ratio = float(two_level_workspace) / float(flat_workspace)
        gate_check(
            checks,
            "two_level_rabitq.workspace_ratio",
            workspace_ratio is not None and workspace_ratio <= args.gate_max_two_level_workspace_vs_flat,
            f"two_level search workspace bytes / flat search workspace bytes must be <= {args.gate_max_two_level_workspace_vs_flat}",
            workspace_ratio,
        )
        for label, row in (
            ("read_flat_rabitq", flat_read),
            ("read_two_level_rabitq", two_level_read),
        ):
            gate_check(
                checks,
                f"{label}.record_directory_skips_hbc_pinning",
                numeric_at_most(row.get("profile_upper_tree_pin_ns"), args.gate_max_directory_upper_tree_pin_ns),
                f"record-backed directory reads must keep upper-tree pin time <= {args.gate_max_directory_upper_tree_pin_ns}",
                row.get("profile_upper_tree_pin_ns"),
            )

    passed = all(bool(check["passed"]) for check in checks)
    return {
        "passed": passed,
        "checks": checks,
    }


def print_gate_report(report: dict[str, Any]) -> None:
    status = "PASS" if report.get("passed") else "FAIL"
    print(f"# optimized gate: {status}")
    print("check\tpassed\tvalue\tdetail")
    for check in report["checks"]:
        print(
            "\t".join(
                [
                    str(check["name"]),
                    "true" if check["passed"] else "false",
                    format_value(check.get("value")),
                    str(check["detail"]),
                ]
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=DEFAULT_RESULT_FILE,
        help=f"comparison JSONL file (default: {DEFAULT_RESULT_FILE})",
    )
    parser.add_argument(
        "--format",
        choices=("tsv", "markdown", "json"),
        default="tsv",
        help="output format",
    )
    parser.add_argument(
        "--kind",
        choices=("all", "write", "read"),
        default="all",
        help="rows to include",
    )
    parser.add_argument(
        "--optimized-gate",
        choices=("off", "report", "check"),
        default="off",
        help="evaluate pass/fail criteria for calling the SPFresh/HBC comparison optimized",
    )
    parser.add_argument("--gate-min-vectors", type=float, default=1_000_000, help="minimum vector count required by the optimized gate")
    parser.add_argument("--gate-min-local-vectors", type=float, default=128, help="minimum vector count required for local non-VDBB diagnostic rows")
    parser.add_argument("--gate-max-recall-drop", type=float, default=0.05, help="maximum allowed recall delta loss versus packed HBC")
    parser.add_argument("--gate-min-post-write-recall-total", type=float, default=40, help="minimum exact post-write recall denominator for required write rows")
    parser.add_argument("--gate-min-vdbb-1m-recall-total", type=float, default=1000, help="minimum exact post-write recall denominator for 1M VDBB-shaped write rows")
    parser.add_argument("--gate-max-read-recall-drop", type=float, default=0.05, help="maximum allowed read-bench recall delta loss versus packed HBC")
    parser.add_argument("--gate-min-read-recall-total", type=float, default=1000, help="minimum read-bench recall denominator for required read rows")
    parser.add_argument("--gate-min-qps-vs-packed", type=float, default=0.80, help="minimum allowed QPS ratio versus packed HBC")
    parser.add_argument("--gate-max-p95-vs-packed", type=float, default=1.25, help="maximum allowed post-write p95 query latency ratio versus packed HBC")
    parser.add_argument("--gate-max-warm-internal-quantized-misses", type=float, default=0, help="maximum warm post-write internal quantized payload misses for optimized write rows")
    parser.add_argument("--gate-max-same-leaf-warm-read-ranges", type=float, default=0, help="maximum warm post-write range reads for same-leaf optimized write rows")
    parser.add_argument("--gate-allow-skip-vector-store", action="store_true", help="allow skip-vector-store rows in the optimized gate for external-vector experiments")
    parser.add_argument("--gate-max-auto-maintenance-postings", type=float, default=1024, help="maximum postings an automatic maintenance pass may repair")
    parser.add_argument("--gate-max-auto-delta-tail-postings", type=float, default=1024, help="maximum delta-tail postings allowed by automatic maintenance policy")
    parser.add_argument("--gate-min-auto-delta-records-to-fold", type=float, default=2, help="minimum automatic-maintenance delta-record fold threshold")
    parser.add_argument("--gate-min-auto-tombstone-records-to-fold", type=float, default=1, help="minimum automatic-maintenance tombstone-record fold threshold")
    parser.add_argument("--gate-max-pre-repair-recall-drop", type=float, default=0.20, help="maximum allowed pre-repair recall delta loss versus packed HBC for non-bulk optimized write rows")
    parser.add_argument("--gate-min-pre-repair-qps-vs-packed", type=float, default=0.50, help="minimum allowed pre-repair QPS ratio versus packed HBC for non-bulk optimized write rows")
    parser.add_argument("--gate-max-pre-repair-p95-vs-packed", type=float, default=2.00, help="maximum allowed pre-repair p95 query latency ratio versus packed HBC for non-bulk optimized write rows")
    parser.add_argument("--gate-max-read-p95-vs-packed", type=float, default=1.25, help="maximum allowed read-bench p95 query latency ratio versus packed HBC")
    parser.add_argument("--gate-max-fg-node-bytes-vs-packed", type=float, default=0.50, help="maximum foreground node bytes ratio for overwrite rows")
    parser.add_argument("--gate-max-foreground-splits", type=float, default=0, help="maximum foreground split count for overwrite rows")
    parser.add_argument("--gate-max-storage-write-bytes-vs-packed", type=float, default=1.50, help="maximum storage write byte ratio versus packed HBC for overwrite rows")
    parser.add_argument("--gate-max-lsm-run-bytes-vs-packed", type=float, default=1.50, help="maximum LSM run byte ratio versus packed HBC for overwrite rows")
    parser.add_argument("--gate-max-storage-write-files-vs-packed", type=float, default=1.50, help="maximum storage write file count ratio versus packed HBC for overwrite rows")
    parser.add_argument("--gate-max-lsm-runs-vs-packed", type=float, default=1.50, help="maximum LSM run count ratio versus packed HBC for overwrite rows")
    parser.add_argument("--gate-max-logical-write-puts-vs-packed", type=float, default=0.75, help="maximum logical LSM put-call ratio versus packed HBC for overwrite rows")
    parser.add_argument("--gate-max-backlog-dirty", type=float, default=0, help="maximum dirty posting debt after comparison rows")
    parser.add_argument("--gate-max-backlog-overfull", type=float, default=0, help="maximum overfull posting debt after comparison rows")
    parser.add_argument("--gate-max-backlog-over-capacity-members", type=float, default=0, help="maximum members over capacity after comparison rows")
    parser.add_argument("--gate-max-backlog-at-capacity", type=float, default=1024, help="maximum full-but-not-overfull posting slack debt after comparison rows")
    parser.add_argument("--gate-max-backlog-delta-tails", type=float, default=1024, help="maximum postings with remaining delta-tail debt after comparison rows")
    parser.add_argument("--gate-max-deferred-append-posting-debt", type=float, default=128, help="maximum dirty/delta/overfull posting debt allowed for append-heavy rows that defer split maintenance")
    parser.add_argument("--gate-max-deferred-append-over-capacity-members", type=float, default=2, help="maximum over-capacity members allowed for append-heavy rows that defer split maintenance")
    parser.add_argument("--gate-min-delta-records-per-append", type=float, default=1.0, help="minimum foreground delta records per physical append when delta records are emitted")
    parser.add_argument("--gate-min-grouped-delta-records-per-append", type=float, default=2.0, help="minimum foreground delta records per physical append for append-streaming grouped-delta optimized rows")
    parser.add_argument("--gate-max-grouped-delta-value-bytes-vs-legacy", type=float, default=0.95, help="maximum grouped foreground delta value bytes versus legacy v1 value bytes once physical appends average at least three records")
    parser.add_argument("--gate-max-fold-base-bytes-per-record", type=float, default=1024, help="maximum replacement base value bytes per folded delta record")
    parser.add_argument("--gate-max-fold-base-tail-ratio", type=float, default=32, help="maximum replacement base value bytes divided by deleted delta-tail value bytes")
    parser.add_argument("--gate-min-overlay-cache-bytes", type=float, default=1, help="minimum configured posting overlay cache bytes for optimized base-delta rows")
    parser.add_argument("--gate-max-overlay-cache-bytes", type=float, default=64 * 1024 * 1024, help="maximum configured posting overlay cache bytes for optimized base-delta rows")
    parser.add_argument("--gate-min-write-overlay-cache-hit-rate", type=float, default=0.80, help="minimum warm post-write posting overlay cache hit rate when overlay lookups occur")
    parser.add_argument("--gate-min-read-overlay-cache-hit-rate", type=float, default=0.80, help="minimum warm read posting overlay cache hit rate for optimized base-delta rows")
    parser.add_argument("--gate-max-overlay-cache-admission-skips", type=float, default=0, help="maximum posting overlay cache admission skips for optimized rows")
    parser.add_argument("--gate-max-two-level-posting-centroid-ratio", type=float, default=0.50, help="maximum two-level/flat posting-centroid evaluation ratio")
    parser.add_argument("--gate-max-two-level-block-centroid-ratio", type=float, default=0.10, help="maximum two-level coarse-block centroid evaluation ratio versus flat posting-centroid evaluation")
    parser.add_argument("--gate-max-two-level-block-select-ratio", type=float, default=0.50, help="maximum selected/scanned block ratio for two-level directory")
    parser.add_argument("--gate-max-two-level-workspace-vs-flat", type=float, default=1.25, help="maximum two-level/flat search workspace byte ratio")
    parser.add_argument("--gate-min-two-level-centroid-blocks", type=float, default=256, help="minimum aggregated scanned centroid block count required to prove two-level directory pruning")
    parser.add_argument("--gate-max-directory-upper-tree-pin-ns", type=float, default=0, help="maximum upper-tree pin time allowed for record-backed directory query rows")
    parser.add_argument("--gate-production-proof-file", type=Path, default=None, help="log captured from `zig build spfresh-production-test --summary all`; required for optimized-gate success")
    args = parser.parse_args()

    rows = load_rows(args.path)
    write_rows = summarize_write(rows) if args.kind in ("all", "write") or args.optimized_gate != "off" else []
    read_rows = summarize_read(rows) if args.kind in ("all", "read") or args.optimized_gate != "off" else []

    if args.optimized_gate != "off":
        report = evaluate_optimized_gate(args, write_rows, read_rows)
        if args.format == "json":
            print(json.dumps({"optimized_gate": report, "write": write_rows, "read": read_rows}, indent=2, sort_keys=True))
        else:
            print_gate_report(report)
        if args.optimized_gate == "check" and not report["passed"]:
            return 1
        return 0

    if args.format == "json":
        print(json.dumps({"write": write_rows, "read": read_rows}, indent=2, sort_keys=True))
        return 0

    printer = print_tsv if args.format == "tsv" else print_markdown
    if write_rows:
        printer("write rows", WRITE_COLUMNS, write_rows)
    if write_rows and read_rows:
        print()
    if read_rows:
        printer("read rows", READ_COLUMNS, read_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
