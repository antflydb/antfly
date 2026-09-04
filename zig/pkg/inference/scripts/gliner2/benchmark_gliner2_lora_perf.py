#!/usr/bin/env python3
"""Run repeated GLiNER2 LoRA Python/Zig comparisons and summarize medians."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_ORACLE_PACKAGE_VERSIONS,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
)


def script_dir() -> Path:
    return Path(__file__).resolve().parent


def repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    pos = (len(ordered) - 1) * pct
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def metric(summary: dict[str, Any], key: str) -> float | None:
    value = summary.get(key)
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def summarize(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "median": None, "p90": None, "min": None, "max": None}
    return {
        "count": len(values),
        "median": statistics.median(values),
        "p90": percentile(values, 0.90),
        "min": min(values),
        "max": max(values),
    }


def aggregate_top_dot_shape_families(
    top_shapes: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}
    for shape in top_shapes:
        if not isinstance(shape, dict):
            continue
        phase = str(shape.get("phase") or "unknown")
        family = str(shape.get("family") or "unknown")
        key = f"{phase}:{family}"
        entry = families.setdefault(key, {"count": 0, "total_ms": 0.0, "avg_ms": None})
        try:
            entry["count"] += int(shape.get("count") or 0)
        except (TypeError, ValueError):
            pass
        try:
            entry["total_ms"] += float(shape.get("total_ms") or 0.0)
        except (TypeError, ValueError):
            pass
    for entry in families.values():
        count = entry["count"]
        entry["total_ms"] = round(float(entry["total_ms"]), 3)
        entry["avg_ms"] = round(float(entry["total_ms"]) / count, 6) if count else None
    return dict(
        sorted(families.items(), key=lambda item: item[1]["total_ms"], reverse=True)
    )


def first_summary_list(
    successful: list[dict[str, Any]], key: str
) -> list[dict[str, Any]]:
    return next(
        (run["summary"].get(key) for run in successful if run["summary"].get(key)), []
    )


COMMAND_DISPATCH_FAMILY_KEYS = [
    ("dot_general", "zig_metal_command_dot_general_dispatches_avg"),
    ("head_dot", "zig_metal_command_head_dot_dispatches_avg"),
    ("transpose", "zig_metal_command_transpose_dispatches_avg"),
    ("gather", "zig_metal_command_gather_dispatches_avg"),
    ("reduce", "zig_metal_command_reduce_dispatches_avg"),
    ("elementwise", "zig_metal_command_elementwise_dispatches_avg"),
    ("activation", "zig_metal_command_activation_dispatches_avg"),
    ("activation_backward", "zig_metal_command_activation_backward_dispatches_avg"),
    ("other", "zig_metal_command_other_dispatches_avg"),
]


def top_command_dispatch_families(
    metrics: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    families: list[dict[str, Any]] = []
    for name, key in COMMAND_DISPATCH_FAMILY_KEYS:
        median = metrics.get(key, {}).get("median")
        if median is None or median <= 0:
            continue
        families.append({"family": name, "median": median})
    return sorted(families, key=lambda item: item["median"], reverse=True)


def run_compare(
    argv: list[str],
    out_dir: Path,
    run_index: int,
    timeout: int | None,
    env: dict[str, str] | None,
) -> dict[str, Any]:
    run_dir = out_dir / f"run-{run_index:02d}"
    report_path = run_dir / "comparison_report.json"
    report_path.unlink(missing_ok=True)
    cmd = [
        sys.executable,
        str(script_dir() / "compare_gliner2_lora_python_zig.py"),
        "--out-dir",
        str(run_dir),
        *argv,
    ]
    started = time.time()
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root()),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        env=run_env,
    )
    report: dict[str, Any] = {}
    report_error: str | None = None
    try:
        loaded = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict) or not isinstance(loaded.get("summary"), dict):
            raise ValueError("comparison report must contain a summary object")
        report = loaded
    except (FileNotFoundError, OSError, json.JSONDecodeError, ValueError) as exc:
        report_error = f"missing or invalid current comparison report: {exc}"
    returncode = proc.returncode
    if returncode == 0 and report_error is not None:
        returncode = 1
    return {
        "run": run_index,
        "returncode": returncode,
        "elapsed_seconds": time.time() - started,
        "argv": cmd,
        "report_path": str(report_path),
        "summary": report.get("summary", {}),
        "output_tail": (proc.stdout + (f"\n{report_error}" if report_error else ""))[
            -8000:
        ],
    }


def args_with_seed(argv: list[str], seed: int) -> list[str]:
    result = list(argv)
    try:
        index = result.index("--seed")
    except ValueError:
        result.extend(["--seed", str(seed)])
        return result
    if index + 1 >= len(result):
        raise ValueError("forwarded --seed is missing its value")
    result[index + 1] = str(seed)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repeat compare_gliner2_lora_python_zig.py and write median/p90 performance summary.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/private/tmp/termite-gliner2-lora-perf-runs"),
    )
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--allow-failures", action="store_true")
    parser.add_argument(
        "--seeds",
        default=None,
        help="Comma-separated isolated comparison seeds; count must equal --runs",
    )
    parser.add_argument(
        "--op-stats",
        action="store_true",
        help="Set TERMITE_METAL_PARTITION_OP_STATS=1 for each comparison run",
    )
    parser.add_argument(
        "--op-runs",
        action="store_true",
        help="Set TERMITE_METAL_PARTITION_OP_RUNS=1 for grouped-dot candidate summaries",
    )
    parser.add_argument(
        "--loop-profile",
        action="store_true",
        help="Set TERMITE_METAL_PARTITION_LOOP_PROFILE=1 for executor loop timing summaries",
    )
    parser.add_argument(
        "--hazard-profile",
        action="store_true",
        help="Set TERMITE_METAL_PLANNED_ACCESS_PROFILE=1 for planned-access hazard timing summaries",
    )
    parser.add_argument("--max-zig-median-ms", type=float, default=None)
    parser.add_argument("--max-zig-python-step-ratio-median", type=float, default=None)
    parser.add_argument("--max-zig-python-step-ratio-any-run", type=float, default=None)
    parser.add_argument(
        "--max-zig-python-warm-step-ratio-median", type=float, default=None
    )
    parser.add_argument(
        "--max-zig-python-warm-step-ratio-any-run", type=float, default=None
    )
    parser.add_argument("--max-host-output-median", type=float, default=None)
    parser.add_argument("--max-true-host-output-median", type=float, default=None)
    parser.add_argument(
        "--max-parameter-materialization-median", type=float, default=None
    )
    parser.add_argument(
        "--warn-parameter-materialization-median", type=float, default=None
    )
    parser.add_argument("--max-fallback-median", type=float, default=None)
    parser.add_argument("--max-command-dispatch-median", type=float, default=None)
    parser.add_argument("--max-dot-general-count-median", type=float, default=None)
    parser.add_argument("--max-dot-general-ms-median", type=float, default=None)
    parser.add_argument("--max-gather-host-output-median", type=float, default=None)
    parser.add_argument(
        "--max-zig-metal-peak-live-bytes-median", type=float, default=None
    )
    parser.add_argument(
        "--max-zig-metal-planned-barriers-median", type=float, default=None
    )
    parser.add_argument(
        "--max-zig-metal-planned-scopes-median", type=float, default=None
    )
    parser.add_argument(
        "--max-zig-low-rank-lora-backward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-residual-layernorm-region-median", type=float, default=None
    )
    parser.add_argument("--max-zig-scaffold-region-median", type=float, default=None)
    parser.add_argument(
        "--max-zig-encoder-lora-fallback-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-lora-backward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-low-rank-lora-backward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-rank-adapter-backward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-ffn-gelu-backward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-head-mlp-forward-region-median", type=float, default=None
    )
    parser.add_argument(
        "--min-zig-head-mlp-backward-region-median", type=float, default=None
    )
    parser.add_argument("--warn-zig-median-ms", type=float, default=None)
    parser.add_argument("--require-loss-parity", action="store_true")
    parser.add_argument("--require-adapter-roundtrip", action="store_true")
    parser.add_argument("--require-zig-beats-python", action="store_true")
    parser.add_argument(
        "compare_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to compare_gliner2_lora_python_zig.py. Prefix with -- before forwarded args.",
    )
    args = parser.parse_args()

    if args.runs <= 0:
        parser.error("--runs must be positive")
    seeds: list[int] | None = None
    if args.seeds is not None:
        try:
            seeds = [
                int(value.strip()) for value in args.seeds.split(",") if value.strip()
            ]
        except ValueError as exc:
            parser.error(f"--seeds must contain integers: {exc}")
        if len(seeds) != args.runs:
            parser.error("--seeds count must equal --runs")
    for name, value in vars(args).items():
        if (
            name.startswith(("max_", "min_", "warn_"))
            and value is not None
            and not math.isfinite(value)
        ):
            parser.error(f"--{name.replace('_', '-')} must be finite")
    forwarded = args.compare_args
    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    runs: list[dict[str, Any]] = []
    run_env: dict[str, str] = {}
    if args.op_stats:
        run_env["TERMITE_METAL_PARTITION_OP_STATS"] = "1"
    if args.op_runs:
        run_env["TERMITE_METAL_PARTITION_OP_RUNS"] = "1"
    if args.loop_profile:
        run_env["TERMITE_METAL_PARTITION_LOOP_PROFILE"] = "1"
    if args.hazard_profile:
        run_env["TERMITE_METAL_PLANNED_ACCESS_PROFILE"] = "1"
    for idx in range(1, args.runs + 1):
        run_args = (
            args_with_seed(forwarded, seeds[idx - 1])
            if seeds is not None
            else forwarded
        )
        result = run_compare(run_args, args.out_dir, idx, args.timeout_seconds, run_env)
        runs.append(result)
        print(
            json.dumps(
                {
                    "event": "perf_run",
                    "run": idx,
                    "returncode": result["returncode"],
                    "summary": result["summary"],
                },
                sort_keys=True,
            )
        )
        if result["returncode"] != 0 and not args.allow_failures:
            break

    successful = [run for run in runs if run["returncode"] == 0]
    python_oracle_expected = "--skip-python" not in forwarded
    oracle_rows = [run["summary"].get("oracle") for run in successful]
    keys = [
        "requested_step_count",
        "python_step_count",
        "python_warm_step_count",
        "zig_step_count",
        "zig_warm_step_count",
        "python_avg_step_wall_ms",
        "python_warm_avg_step_wall_ms",
        "zig_avg_trainer_ms",
        "zig_warm_avg_trainer_ms",
        "zig_total_trainer_ms",
        "zig_epoch_wall_ms",
        "zig_graph_executor_command_dispatches_avg",
        "zig_graph_executor_interpreter_fallbacks_avg",
        "zig_graph_executor_host_outputs_avg",
        "zig_graph_executor_true_host_outputs_avg",
        "zig_graph_executor_host_output_command_avg",
        "zig_graph_executor_host_output_interpreter_avg",
        "zig_graph_executor_host_output_pre_materialized_constant_avg",
        "zig_graph_executor_host_output_runtime_region_avg",
        "zig_graph_executor_host_output_parameter_avg",
        "zig_graph_executor_parameter_materializations_avg",
        "zig_graph_executor_host_output_unattributed_avg",
        "zig_graph_executor_device_output_parameter_avg",
        "zig_cuda_device_allocations_avg",
        "zig_cuda_device_frees_avg",
        "zig_cuda_h2d_bytes_avg",
        "zig_cuda_h2d_bytes_max",
        "zig_cuda_training_input_uploads_avg",
        "zig_cuda_training_input_upload_bytes_avg",
        "zig_cuda_training_input_upload_bytes_max",
        "zig_cuda_d2h_bytes_avg",
        "zig_cuda_d2h_bytes_max",
        "zig_cuda_largest_d2h_transfer_bytes_max",
        "zig_cuda_to_float32_calls_avg",
        "zig_cuda_to_float32_calls_max",
        "zig_cuda_stream_synchronizations_avg",
        "zig_cuda_upload_synchronizations_avg",
        "zig_cuda_temp_cache_hits_avg",
        "zig_cuda_temp_cache_misses_avg",
        "zig_cuda_kernel_launches_avg",
        "zig_graph_executor_metal_gather_input_promotions_avg",
        "zig_graph_executor_metal_gather_input_promotion_bytes_avg",
        "zig_graph_executor_metal_gather_input_promotion_ms_avg",
        "zig_graph_executor_metal_gather_output_promotions_avg",
        "zig_graph_executor_metal_gather_output_promotion_bytes_avg",
        "zig_graph_executor_metal_gather_output_promotion_ms_avg",
        "zig_graph_executor_metal_reduce_input_promotions_avg",
        "zig_graph_executor_metal_reduce_input_promotion_bytes_avg",
        "zig_graph_executor_metal_reduce_input_promotion_ms_avg",
        "zig_graph_executor_metal_resident_input_cache_hits_avg",
        "zig_graph_executor_metal_resident_input_cache_misses_avg",
        "zig_graph_executor_metal_resident_input_cache_unique_promotions_avg",
        "zig_graph_executor_metal_resident_input_cache_retained_live_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_retained_peak_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_reused_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_released_bytes_avg",
        "zig_graph_executor_regions_avg",
        "zig_graph_executor_runtime_region_dispatches_avg",
        "zig_graph_executor_runtime_region_active_regions_avg",
        "zig_graph_executor_runtime_region_covered_nodes_avg",
        "zig_graph_executor_runtime_region_elided_nodes_avg",
        "zig_graph_executor_runtime_region_plan_compiles_avg",
        "zig_graph_executor_runtime_region_plan_reuses_avg",
        "zig_graph_executor_runtime_frame_candidates_avg",
        "zig_graph_executor_runtime_frame_eligible_avg",
        "zig_graph_executor_runtime_frame_metadata_ready_avg",
        "zig_graph_executor_runtime_frame_ineligible_no_regions_avg",
        "zig_graph_executor_runtime_frame_ineligible_missing_qkv_avg",
        "zig_graph_executor_runtime_frame_ineligible_missing_attention_avg",
        "zig_graph_executor_runtime_frame_ineligible_missing_ffn_avg",
        "zig_graph_executor_runtime_frame_ineligible_missing_ple_avg",
        "zig_graph_executor_runtime_frame_ineligible_single_row_avg",
        "zig_graph_executor_runtime_frame_ineligible_non_layer_order_avg",
        "zig_graph_executor_runtime_frame_ineligible_shape_mismatch_avg",
        "zig_graph_executor_runtime_frame_ineligible_missing_model_metadata_avg",
        "zig_graph_executor_plan_build_ms_avg",
        "zig_graph_executor_buffer_plan_build_ms_avg",
        "zig_graph_executor_plan_cache_hits_avg",
        "zig_graph_executor_plan_cache_misses_avg",
        "zig_metal_frame_wait_ms_avg",
        "zig_metal_frame_gpu_ms_avg",
        "zig_metal_tensor_device_owned_peak_live_bytes_avg",
        "zig_metal_tensor_device_owned_live_bytes_avg",
        "zig_metal_runtime_total_bytes_avg",
        "zig_metal_last_frame_compute_encoders_avg",
        "zig_metal_last_frame_blit_encoders_avg",
        "zig_metal_last_frame_planned_scopes_avg",
        "zig_metal_last_frame_planned_barriers_avg",
        "zig_metal_last_frame_planned_command_ops_avg",
        "zig_metal_deberta_encoder_plan_attempts_avg",
        "zig_metal_deberta_encoder_plan_successes_avg",
        "zig_metal_deberta_encoder_plan_reuses_avg",
        "zig_metal_deberta_encoder_plan_failures_avg",
        "zig_metal_deberta_encoder_layer_attempts_avg",
        "zig_metal_deberta_encoder_layer_successes_avg",
        "zig_metal_deberta_encoder_layer_fallbacks_avg",
        "zig_metal_deberta_encoder_lora_layer_regions_avg",
        "zig_metal_deberta_encoder_lora_residual_layernorm_regions_avg",
        "zig_metal_deberta_encoder_lora_layer_scaffold_regions_avg",
        "zig_metal_deberta_encoder_lora_layer_fallbacks_avg",
        "zig_metal_lora_backward_regions_avg",
        "zig_metal_low_rank_lora_backward_regions_avg",
        "zig_metal_rank_adapter_backward_regions_avg",
        "zig_metal_lora_backward_total_regions_avg",
        "zig_metal_ffn_gelu_backward_regions_avg",
        "zig_metal_head_mlp_forward_regions_avg",
        "zig_metal_head_mlp_backward_regions_avg",
        "zig_metal_command_dot_general_dispatches_avg",
        "zig_metal_command_head_dot_dispatches_avg",
        "zig_metal_command_transpose_dispatches_avg",
        "zig_metal_command_gather_dispatches_avg",
        "zig_metal_command_reduce_dispatches_avg",
        "zig_metal_command_elementwise_dispatches_avg",
        "zig_metal_command_activation_dispatches_avg",
        "zig_metal_command_activation_backward_dispatches_avg",
        "zig_metal_command_other_dispatches_avg",
        "zig_metal_deberta_relative_qk_pair_calls_avg",
        "zig_metal_deberta_relative_qk_pair_fallbacks_avg",
        "zig_metal_deberta_ffn_fused_calls_avg",
        "zig_metal_deberta_ffn_fused_mps_matmuls_avg",
        "zig_metal_deberta_ffn_fused_fallbacks_avg",
        "zig_metal_deberta_attention_flash_calls_avg",
        "zig_metal_deberta_attention_gemm_calls_avg",
        "zig_metal_deberta_attention_gemm_fallbacks_avg",
        "zig_metal_deberta_attention_legacy_calls_avg",
        "zig_dot_general_command_count",
        "zig_dot_general_command_total_ms",
        "zig_dot_general_command_avg_ms",
        "zig_gather_fallback_count",
        "zig_gather_fallback_total_ms",
        "zig_gather_host_output_count",
        "zig_gather_host_output_total_ms",
        "zig_loop_profile_count",
        "zig_loop_profile_warm_count",
        "zig_loop_profile_execution_ms_avg",
        "zig_loop_profile_warm_execution_ms_avg",
        "zig_loop_profile_planned_region_ms_avg",
        "zig_loop_profile_warm_planned_region_ms_avg",
        "zig_loop_profile_fused_pattern_ms_avg",
        "zig_loop_profile_warm_fused_pattern_ms_avg",
        "zig_loop_profile_command_path_ms_avg",
        "zig_loop_profile_warm_command_path_ms_avg",
        "zig_loop_profile_interpreter_ms_avg",
        "zig_loop_profile_warm_interpreter_ms_avg",
        "zig_loop_profile_stats_ms_avg",
        "zig_loop_profile_warm_stats_ms_avg",
        "zig_loop_profile_alias_clone_ms_avg",
        "zig_loop_profile_warm_alias_clone_ms_avg",
        "zig_loop_profile_free_expired_ms_avg",
        "zig_loop_profile_warm_free_expired_ms_avg",
        "zig_loop_profile_submit_frame_ms_avg",
        "zig_loop_profile_warm_submit_frame_ms_avg",
        "zig_loop_profile_accounted_ms_avg",
        "zig_loop_profile_warm_accounted_ms_avg",
        "zig_loop_profile_command_path_hits_avg",
        "zig_loop_profile_warm_command_path_hits_avg",
        "zig_planned_access_profile_count",
        "zig_planned_access_profile_warm_count",
        "zig_planned_access_profile_calls_avg",
        "zig_planned_access_profile_warm_calls_avg",
        "zig_planned_access_profile_accesses_avg",
        "zig_planned_access_profile_warm_accesses_avg",
        "zig_planned_access_profile_range_scans_avg",
        "zig_planned_access_profile_warm_range_scans_avg",
        "zig_planned_access_profile_conflicts_avg",
        "zig_planned_access_profile_warm_conflicts_avg",
        "zig_planned_access_profile_capacity_flushes_avg",
        "zig_planned_access_profile_warm_capacity_flushes_avg",
        "zig_planned_access_profile_barriers_avg",
        "zig_planned_access_profile_warm_barriers_avg",
        "zig_planned_access_profile_total_ms_avg",
        "zig_planned_access_profile_warm_total_ms_avg",
        "zig_planned_access_profile_avg_us_avg",
        "zig_planned_access_profile_warm_avg_us_avg",
        "zig_final_avg_loss",
        "loss_delta_zig_minus_python",
        "valid_loss_parity",
    ]
    metrics = {
        key: summarize(
            [
                value
                for run in successful
                if (value := metric(run["summary"], key)) is not None
            ]
        )
        for key in keys
    }
    zig_median = metrics["zig_avg_trainer_ms"]["median"]
    py_median = metrics["python_avg_step_wall_ms"]["median"]
    zig_python_step_ratio = (
        zig_median / py_median
        if zig_median is not None and py_median is not None and py_median > 0.0
        else None
    )
    zig_python_step_ratios = []
    for run in successful:
        zig_step = metric(run["summary"], "zig_avg_trainer_ms")
        python_step = metric(run["summary"], "python_avg_step_wall_ms")
        if zig_step is not None and python_step is not None and python_step > 0.0:
            zig_python_step_ratios.append(zig_step / python_step)
    warm_zig_median = metrics["zig_warm_avg_trainer_ms"]["median"]
    warm_py_median = metrics["python_warm_avg_step_wall_ms"]["median"]
    zig_python_warm_step_ratio = (
        warm_zig_median / warm_py_median
        if warm_zig_median is not None
        and warm_py_median is not None
        and warm_py_median > 0.0
        else None
    )
    zig_python_warm_step_ratios = []
    for run in successful:
        zig_step = metric(run["summary"], "zig_warm_avg_trainer_ms")
        python_step = metric(run["summary"], "python_warm_avg_step_wall_ms")
        if zig_step is not None and python_step is not None and python_step > 0.0:
            zig_python_warm_step_ratios.append(zig_step / python_step)
    top_dot_shapes = first_summary_list(successful, "zig_top_dot_shapes")
    step_count_mismatches = [
        {
            "run": run["run"],
            "requested_step_count": run["summary"].get("requested_step_count"),
            "python_step_count": run["summary"].get("python_step_count"),
            "zig_step_count": run["summary"].get("zig_step_count"),
            "python_warm_step_count": run["summary"].get("python_warm_step_count"),
            "zig_warm_step_count": run["summary"].get("zig_warm_step_count"),
            "step_count_match": run["summary"].get("step_count_match"),
            "warm_step_count_match": run["summary"].get("warm_step_count_match"),
            "python_step_count_matches_requested": run["summary"].get(
                "python_step_count_matches_requested"
            ),
            "zig_step_count_matches_requested": run["summary"].get(
                "zig_step_count_matches_requested"
            ),
        }
        for run in successful
        if run["summary"].get("step_count_valid") is not True
    ]
    host_output_accounting_keys = [
        "zig_graph_executor_host_output_command_avg",
        "zig_graph_executor_true_host_outputs_avg",
        "zig_graph_executor_host_output_interpreter_avg",
        "zig_graph_executor_host_output_pre_materialized_constant_avg",
        "zig_graph_executor_host_output_runtime_region_avg",
        "zig_graph_executor_host_output_parameter_avg",
        "zig_graph_executor_host_output_unattributed_avg",
        "zig_graph_executor_device_output_parameter_avg",
        "zig_graph_executor_metal_gather_input_promotions_avg",
        "zig_graph_executor_metal_gather_input_promotion_bytes_avg",
        "zig_graph_executor_metal_gather_input_promotion_ms_avg",
        "zig_graph_executor_metal_gather_output_promotions_avg",
        "zig_graph_executor_metal_gather_output_promotion_bytes_avg",
        "zig_graph_executor_metal_gather_output_promotion_ms_avg",
        "zig_graph_executor_metal_reduce_input_promotions_avg",
        "zig_graph_executor_metal_reduce_input_promotion_bytes_avg",
        "zig_graph_executor_metal_reduce_input_promotion_ms_avg",
        "zig_graph_executor_metal_resident_input_cache_hits_avg",
        "zig_graph_executor_metal_resident_input_cache_misses_avg",
        "zig_graph_executor_metal_resident_input_cache_unique_promotions_avg",
        "zig_graph_executor_metal_resident_input_cache_retained_live_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_retained_peak_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_reused_bytes_avg",
        "zig_graph_executor_metal_resident_input_cache_released_bytes_avg",
    ]
    summary = {
        "runs_requested": args.runs,
        "runs_completed": len(runs),
        "runs_successful": len(successful),
        "seeds": seeds,
        "compare_args": forwarded,
        "metrics": metrics,
        "zig_beats_python_median_step_time": (
            zig_median < py_median
            if zig_median is not None and py_median is not None
            else None
        ),
        "zig_python_step_ratio_median": zig_python_step_ratio,
        "zig_python_step_ratio_max": max(zig_python_step_ratios)
        if zig_python_step_ratios
        else None,
        "zig_python_step_ratios": zig_python_step_ratios,
        "zig_python_warm_step_ratio_median": zig_python_warm_step_ratio,
        "zig_python_warm_step_ratio_max": (
            max(zig_python_warm_step_ratios) if zig_python_warm_step_ratios else None
        ),
        "zig_python_warm_step_ratios": zig_python_warm_step_ratios,
        "step_count_valid": bool(successful) and not step_count_mismatches,
        "step_count_mismatches": step_count_mismatches,
        "op_stats_enabled": args.op_stats,
        "op_runs_enabled": args.op_runs,
        "loop_profile_enabled": args.loop_profile,
        "hazard_profile_enabled": args.hazard_profile,
        "top_dot_shapes": top_dot_shapes,
        "top_dot_shape_families": aggregate_top_dot_shape_families(top_dot_shapes),
        "top_command_dispatch_families": top_command_dispatch_families(metrics),
        "top_host_output_families": first_summary_list(
            successful, "zig_top_host_output_families"
        ),
        "top_fallback_families": first_summary_list(
            successful, "zig_top_fallback_families"
        ),
        "top_host_output_reasons": first_summary_list(
            successful, "zig_top_host_output_reasons"
        ),
        "host_output_accounting": {
            key: metrics[key]["median"]
            for key in host_output_accounting_keys
            if key in metrics
        },
        "run_reports": [run["report_path"] for run in runs],
    }
    accelerator_backends = [
        run["summary"].get("zig_manifest_backend", "").lower() for run in successful
    ]
    training_precisions = [
        run["summary"].get("zig_training_precision") for run in successful
    ]
    optimizer_precisions = [
        run["summary"].get("zig_optimizer_state_precision") for run in successful
    ]
    summary["accelerator_backend"] = (
        accelerator_backends[0] if accelerator_backends else None
    )
    summary["training_precision"] = (
        training_precisions[0] if training_precisions else None
    )
    summary["optimizer_state_precision"] = (
        optimizer_precisions[0] if optimizer_precisions else None
    )
    summary["precision_consistent"] = bool(successful) and (
        len(set(accelerator_backends)) == 1
        and training_precisions == ["fp32"] * len(successful)
        and optimizer_precisions == ["fp32"] * len(successful)
    )
    failures: list[str] = []
    if len(successful) != args.runs:
        failures.append(f"only {len(successful)}/{args.runs} runs succeeded")
    if (
        python_oracle_expected
        and successful
        and summary["precision_consistent"] is not True
    ):
        failures.append(
            "accelerator backend or FP32 training/optimizer precision was missing or inconsistent"
        )
    valid_oracles = [
        oracle
        for oracle in oracle_rows
        if isinstance(oracle, dict)
        and oracle.get("commit") == UPSTREAM_COMMIT
        and str(oracle.get("python_version", "")).startswith("3.12.")
        and oracle.get("unicode_version") == CANONICAL_UNICODE_VERSION
        and oracle.get("gliner2_version") == CANONICAL_GLINER2_VERSION
        and oracle.get("package_versions") == CANONICAL_ORACLE_PACKAGE_VERSIONS
        and isinstance(oracle.get("checkout"), str)
        and isinstance(oracle.get("imported_module"), str)
    ]
    summary["oracle"] = valid_oracles[0] if valid_oracles else None
    summary["oracle_valid_in_every_run"] = not python_oracle_expected or (
        len(valid_oracles) == args.runs
        and len({oracle["checkout"] for oracle in valid_oracles}) == 1
        and len({oracle["imported_module"] for oracle in valid_oracles}) == 1
    )
    if python_oracle_expected and summary["oracle_valid_in_every_run"] is not True:
        failures.append(
            f"Python oracle source/runtime was not pinned to {UPSTREAM_COMMIT} and the canonical dependency set in every run"
        )
    for fingerprint_key in (
        "model_fingerprint_sha256",
        "training_data_fingerprint_sha256",
    ):
        values = [run["summary"].get(fingerprint_key) for run in successful]
        valid_values = [
            value
            for value in values
            if isinstance(value, str)
            and value.startswith("sha256:")
            and len(value) == 71
        ]
        summary[fingerprint_key] = valid_values[0] if valid_values else None
        summary[f"{fingerprint_key}_consistent"] = (
            not python_oracle_expected
            or len(valid_values) == args.runs
            and len(set(valid_values)) == 1
        )
        if (
            python_oracle_expected
            and summary[f"{fingerprint_key}_consistent"] is not True
        ):
            failures.append(
                f"{fingerprint_key} was missing or inconsistent across runs"
            )
    if step_count_mismatches:
        failures.append(
            "step-count mismatch invalidates performance ratios: "
            + "; ".join(
                "run {run} requested={requested_step_count} python={python_step_count} zig={zig_step_count}".format(
                    **row
                )
                for row in step_count_mismatches
            )
        )
    threshold_checks = [
        ("zig_avg_trainer_ms", args.max_zig_median_ms, "Zig median trainer ms"),
        (
            "zig_graph_executor_command_dispatches_avg",
            args.max_command_dispatch_median,
            "command dispatch median",
        ),
        (
            "zig_graph_executor_host_outputs_avg",
            args.max_host_output_median,
            "aggregate host output median",
        ),
        (
            "zig_graph_executor_true_host_outputs_avg",
            args.max_true_host_output_median,
            "true host output median",
        ),
        (
            "zig_graph_executor_parameter_materializations_avg",
            args.max_parameter_materialization_median,
            "parameter materialization median",
        ),
        (
            "zig_graph_executor_interpreter_fallbacks_avg",
            args.max_fallback_median,
            "fallback median",
        ),
        (
            "zig_dot_general_command_count",
            args.max_dot_general_count_median,
            "dot_general command-count median",
        ),
        (
            "zig_dot_general_command_total_ms",
            args.max_dot_general_ms_median,
            "dot_general total-ms median",
        ),
        (
            "zig_gather_host_output_count",
            args.max_gather_host_output_median,
            "gather host-output median",
        ),
        (
            "zig_metal_tensor_device_owned_peak_live_bytes_avg",
            args.max_zig_metal_peak_live_bytes_median,
            "Zig Metal peak-live-bytes median",
        ),
        (
            "zig_metal_last_frame_planned_barriers_avg",
            args.max_zig_metal_planned_barriers_median,
            "Zig Metal planned-barriers median",
        ),
        (
            "zig_metal_last_frame_planned_scopes_avg",
            args.max_zig_metal_planned_scopes_median,
            "Zig Metal planned-scopes median",
        ),
        (
            "zig_metal_deberta_encoder_lora_layer_scaffold_regions_avg",
            args.max_zig_scaffold_region_median,
            "Zig scaffold-region median",
        ),
        (
            "zig_metal_deberta_encoder_lora_layer_fallbacks_avg",
            args.max_zig_encoder_lora_fallback_median,
            "Zig encoder-LoRA fallback median",
        ),
        (
            "zig_metal_low_rank_lora_backward_regions_avg",
            args.max_zig_low_rank_lora_backward_region_median,
            "Zig low-rank LoRA-backward region median",
        ),
    ]
    for key, limit, label in threshold_checks:
        median = metrics[key]["median"]
        if limit is not None and (median is None or median > limit):
            failures.append(f"{label} {median} exceeds limit {limit}")
    if args.max_zig_python_step_ratio_median is not None and (
        zig_python_step_ratio is None
        or zig_python_step_ratio > args.max_zig_python_step_ratio_median
    ):
        failures.append(
            f"Zig/Python median step ratio {zig_python_step_ratio} exceeds limit {args.max_zig_python_step_ratio_median}"
        )
    if args.max_zig_python_warm_step_ratio_median is not None and (
        zig_python_warm_step_ratio is None
        or zig_python_warm_step_ratio > args.max_zig_python_warm_step_ratio_median
    ):
        failures.append(
            "Zig/Python warm median step ratio "
            f"{zig_python_warm_step_ratio} exceeds limit {args.max_zig_python_warm_step_ratio_median}"
        )
    zig_python_step_ratio_max = (
        max(zig_python_step_ratios) if zig_python_step_ratios else None
    )
    if args.max_zig_python_step_ratio_any_run is not None and (
        zig_python_step_ratio_max is None
        or zig_python_step_ratio_max > args.max_zig_python_step_ratio_any_run
    ):
        failures.append(
            f"Zig/Python max per-run step ratio {zig_python_step_ratio_max} exceeds limit {args.max_zig_python_step_ratio_any_run}"
        )
    zig_python_warm_step_ratio_max = (
        max(zig_python_warm_step_ratios) if zig_python_warm_step_ratios else None
    )
    if args.max_zig_python_warm_step_ratio_any_run is not None and (
        zig_python_warm_step_ratio_max is None
        or zig_python_warm_step_ratio_max > args.max_zig_python_warm_step_ratio_any_run
    ):
        failures.append(
            "Zig/Python max per-run warm step ratio "
            f"{zig_python_warm_step_ratio_max} exceeds limit {args.max_zig_python_warm_step_ratio_any_run}"
        )
    valid_loss_values = [run["summary"].get("valid_loss_parity") for run in successful]
    if args.require_loss_parity and (
        len(valid_loss_values) != args.runs
        or not all(value is True for value in valid_loss_values)
    ):
        failures.append(
            "loss parity was required but not all successful runs reported valid_loss_parity=true"
        )
    adapter_roundtrips = [
        (
            run["summary"].get("adapter_roundtrip_ran"),
            run["summary"].get("adapter_roundtrip_ok"),
        )
        for run in successful
    ]
    adapter_roundtrip_valid = len(adapter_roundtrips) == args.runs and all(
        ran is True and ok is True for ran, ok in adapter_roundtrips
    )
    summary["adapter_roundtrip_required"] = args.require_adapter_roundtrip
    summary["adapter_roundtrip_valid"] = adapter_roundtrip_valid
    if args.require_adapter_roundtrip and not adapter_roundtrip_valid:
        failures.append(
            "adapter round-trip was required but did not run and pass in every run"
        )
    trained_adapter_parities = [
        (
            run["summary"].get("trained_adapter_parity_ran"),
            run["summary"].get("trained_adapter_parity_ok"),
        )
        for run in successful
    ]
    trained_adapter_parity_valid = len(trained_adapter_parities) == args.runs and all(
        ran is True and ok is True for ran, ok in trained_adapter_parities
    )
    summary["trained_adapter_parity_required"] = False
    summary["trained_adapter_parity_valid"] = trained_adapter_parity_valid
    summary["trained_adapter_tensor_equality_diagnostic"] = {
        "gating": False,
        "ran_count": sum(ran is True for ran, _ in trained_adapter_parities),
        "within_diagnostic_tolerance_count": sum(
            ok is True for _, ok in trained_adapter_parities
        ),
        "requested_runs": args.runs,
    }
    if (
        args.require_zig_beats_python
        and summary["zig_beats_python_median_step_time"] is not True
    ):
        failures.append("Zig median step time did not beat Python median step time")
    warnings: list[str] = []
    if (
        args.warn_zig_median_ms is not None
        and zig_median is not None
        and zig_median > args.warn_zig_median_ms
    ):
        warnings.append(
            f"Zig median trainer ms {zig_median} exceeds warning threshold {args.warn_zig_median_ms}"
        )
    parameter_materialization_median = metrics[
        "zig_graph_executor_parameter_materializations_avg"
    ]["median"]
    if (
        args.warn_parameter_materialization_median is not None
        and parameter_materialization_median is not None
        and parameter_materialization_median
        > args.warn_parameter_materialization_median
    ):
        warnings.append(
            f"parameter materialization median {parameter_materialization_median} exceeds warning threshold {args.warn_parameter_materialization_median}"
        )
    residual_ln_median = metrics[
        "zig_metal_deberta_encoder_lora_residual_layernorm_regions_avg"
    ]["median"]
    if args.min_zig_residual_layernorm_region_median is not None and (
        residual_ln_median is None
        or residual_ln_median < args.min_zig_residual_layernorm_region_median
    ):
        failures.append(
            f"Zig residual+LayerNorm region median {residual_ln_median} below limit {args.min_zig_residual_layernorm_region_median}"
        )
    lora_backward_median = metrics["zig_metal_lora_backward_total_regions_avg"][
        "median"
    ]
    if args.min_zig_lora_backward_region_median is not None and (
        lora_backward_median is None
        or lora_backward_median < args.min_zig_lora_backward_region_median
    ):
        failures.append(
            f"Zig total LoRA-backward region median {lora_backward_median} below limit {args.min_zig_lora_backward_region_median}"
        )
    low_rank_lora_backward_median = metrics[
        "zig_metal_low_rank_lora_backward_regions_avg"
    ]["median"]
    if args.min_zig_low_rank_lora_backward_region_median is not None and (
        low_rank_lora_backward_median is None
        or low_rank_lora_backward_median
        < args.min_zig_low_rank_lora_backward_region_median
    ):
        failures.append(
            f"Zig low-rank LoRA-backward region median {low_rank_lora_backward_median} below limit {args.min_zig_low_rank_lora_backward_region_median}"
        )
    rank_adapter_backward_median = metrics[
        "zig_metal_rank_adapter_backward_regions_avg"
    ]["median"]
    if args.min_zig_rank_adapter_backward_region_median is not None and (
        rank_adapter_backward_median is None
        or rank_adapter_backward_median
        < args.min_zig_rank_adapter_backward_region_median
    ):
        failures.append(
            f"Zig rank-adapter-backward region median {rank_adapter_backward_median} below limit {args.min_zig_rank_adapter_backward_region_median}"
        )
    ffn_gelu_backward_median = metrics["zig_metal_ffn_gelu_backward_regions_avg"][
        "median"
    ]
    if args.min_zig_ffn_gelu_backward_region_median is not None and (
        ffn_gelu_backward_median is None
        or ffn_gelu_backward_median < args.min_zig_ffn_gelu_backward_region_median
    ):
        failures.append(
            f"Zig FFN GELU-backward region median {ffn_gelu_backward_median} below limit {args.min_zig_ffn_gelu_backward_region_median}"
        )
    head_mlp_forward_median = metrics["zig_metal_head_mlp_forward_regions_avg"][
        "median"
    ]
    if args.min_zig_head_mlp_forward_region_median is not None and (
        head_mlp_forward_median is None
        or head_mlp_forward_median < args.min_zig_head_mlp_forward_region_median
    ):
        failures.append(
            f"Zig head-MLP forward region median {head_mlp_forward_median} below limit {args.min_zig_head_mlp_forward_region_median}"
        )
    head_mlp_backward_median = metrics["zig_metal_head_mlp_backward_regions_avg"][
        "median"
    ]
    if args.min_zig_head_mlp_backward_region_median is not None and (
        head_mlp_backward_median is None
        or head_mlp_backward_median < args.min_zig_head_mlp_backward_region_median
    ):
        failures.append(
            f"Zig head-MLP backward region median {head_mlp_backward_median} below limit {args.min_zig_head_mlp_backward_region_median}"
        )
    summary["pass"] = not failures
    summary["failures"] = failures
    summary["warnings"] = warnings
    out_path = args.out_dir / "perf_summary.json"
    out_path.write_text(
        json.dumps({"summary": summary, "runs": runs}, indent=2), encoding="utf-8"
    )
    print(f"perf summary: {out_path}")
    print(json.dumps(summary, indent=2))
    return 0 if summary["pass"] or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
