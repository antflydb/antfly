#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Fail-closed BF16 Transformers versus quantized Metal Qwen3-VL reranker gate."""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
import platform
import sys
import time
from typing import Any

from qualify_qwen3vl_metal import (
    FORBIDDEN_RUNTIME_OUTPUT,
    QualificationError,
    git_provenance,
    load_json,
    run_resource_monitored,
    validate_managed_reranker_bundle,
    write_json_atomic,
)
from convert_qwen3vl_reranker import validate_published_bundle


SCHEMA = "antfly.qwen3vl.reranker_metal_qualification.v1"
SCORE_LIMITS = {
    "Q8_0": {
        "max_score_abs": 0.03,
        "max_logit_abs": 0.10,
    },
    # Q4 is a ranking-only tier. These broad bounds detect corruption but do
    # not establish calibrated probability parity.
    "Q4_K_M": {
        "max_score_abs": 0.20,
        "max_logit_abs": 1.00,
    },
}
DEFAULT_QUERY = "Which planet is known as the Red Planet?"
DEFAULT_DOCUMENTS = (
    "Mars is known as the Red Planet because iron minerals in its soil oxidize.",
    "Venus is the hottest planet and has a dense carbon dioxide atmosphere.",
    "Jupiter is the largest planet in the Solar System.",
)


def stable_sigmoid(value: float) -> float:
    if not math.isfinite(value):
        raise QualificationError("non-finite reranker logit")
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-value))
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


def stable_ranking(scores: list[float]) -> list[int]:
    return sorted(range(len(scores)), key=lambda index: (-scores[index], index))


def active_token_rows(request: dict[str, Any]) -> list[list[int]]:
    ids = request.get("input_ids")
    masks = request.get("attention_mask")
    if not isinstance(ids, list) or not isinstance(masks, list) or len(ids) != len(masks):
        raise QualificationError("oracle token/mask evidence is malformed")
    rows: list[list[int]] = []
    for token_row, mask_row in zip(ids, masks, strict=True):
        if not isinstance(token_row, list) or not isinstance(mask_row, list) or len(token_row) != len(mask_row):
            raise QualificationError("oracle token/mask row is malformed")
        rows.append([int(token) for token, mask in zip(token_row, mask_row, strict=True) if int(mask) == 1])
    return rows


def run_oracle(args: argparse.Namespace, work_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    output = work_dir / "transformers_reranker.json"
    hidden = work_dir / "transformers_final_hidden.f32le"
    command = [
        sys.executable,
        str(args.oracle_script),
        "--model-dir",
        str(args.oracle_model_dir),
        "--query",
        args.query,
        "--instruction",
        args.instruction,
        "--threads",
        str(args.oracle_threads),
        "--device",
        "cpu",
        "--hidden-output",
        str(hidden),
        "--output",
        str(output),
    ]
    if args.image is not None:
        command.extend(("--image", str(args.image), "--max-merged-tokens", "576"))
    for document in args.document:
        command.extend(("--document", document))
    env = os.environ.copy()
    env.update(
        {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
        }
    )
    execution = run_resource_monitored(
        command,
        work_dir / "transformers.stdout.log",
        work_dir / "transformers.stderr.log",
        timeout_seconds=args.oracle_timeout_seconds,
        max_rss_mib=args.oracle_max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
        env=env,
        label="Transformers reranker oracle",
    )
    if execution["returncode"] != 0:
        raise QualificationError(
            f"Transformers reranker oracle exited {execution['returncode']}: "
            f"{execution['stderr'].strip()[-2000:]}"
        )
    return load_json(output), {"command": command, **execution, "hidden": str(hidden)}


def run_metal(
    args: argparse.Namespace,
    work_dir: Path,
    repeat_index: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    evidence_path = work_dir / f"antfly_metal_{repeat_index}.json"
    command = [
        str(args.antfly_bin),
        "rerank",
        str(args.model_dir),
        "--query",
        args.query,
    ]
    if args.image is not None:
        command.extend(("--image", str(args.image)))
    for document in args.document:
        command.extend(("--doc", document))
    command.extend(
        (
            "--backend",
            "metal",
            "--qwen3vl-qualification-json",
            str(evidence_path),
        )
    )
    execution = run_resource_monitored(
        command,
        work_dir / f"antfly_metal_{repeat_index}.stdout.log",
        work_dir / f"antfly_metal_{repeat_index}.stderr.log",
        timeout_seconds=args.metal_timeout_seconds,
        max_rss_mib=args.metal_max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
        env=os.environ.copy(),
        label=f"Antfly Metal reranker repeat {repeat_index}",
    )
    if execution["returncode"] != 0:
        raise QualificationError(
            f"Antfly Metal reranker repeat {repeat_index} exited {execution['returncode']}: "
            f"{execution['stderr'].strip()[-2000:]}"
        )
    return load_json(evidence_path), {"command": command, **execution, "evidence": str(evidence_path)}


def add_gate(gates: list[dict[str, Any]], name: str, passed: bool, evidence: object) -> None:
    gates.append({"name": name, "pass": bool(passed), "evidence": evidence})


def evaluate(
    args: argparse.Namespace,
    oracle: dict[str, Any],
    metal_runs: list[dict[str, Any]],
    oracle_execution: dict[str, Any],
    metal_executions: list[dict[str, Any]],
) -> tuple[bool, list[dict[str, Any]], dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    oracle_output = oracle.get("output", {})
    oracle_request = oracle.get("request", {})
    oracle_scores = [float(value) for value in oracle_output.get("scores", [])]
    oracle_logits = [float(value) for value in oracle_output.get("score_logits", [])]
    oracle_prompts = oracle_request.get("rendered_prompts", [])
    oracle_ids = active_token_rows(oracle_request)
    oracle_mrope = oracle_request.get("active_mrope_position_ids", [])
    oracle_image = oracle_request.get("image")
    expected_count = len(args.document)
    multimodal = args.image is not None
    limits = SCORE_LIMITS[args.decoder_quantization]

    add_gate(gates, "oracle_output_count", len(oracle_scores) == expected_count and len(oracle_logits) == expected_count, {
        "scores": len(oracle_scores), "logits": len(oracle_logits), "expected": expected_count
    })
    add_gate(gates, "oracle_values_finite", all(math.isfinite(value) for value in oracle_scores + oracle_logits), {
        "scores": oracle_scores, "logits": oracle_logits
    })
    add_gate(gates, "oracle_sigmoid_contract", all(
        abs(stable_sigmoid(logit) - score) <= 1e-6
        for logit, score in zip(oracle_logits, oracle_scores, strict=True)
    ), {"max_abs": max((abs(stable_sigmoid(logit) - score) for logit, score in zip(oracle_logits, oracle_scores, strict=True)), default=0.0)})

    per_run: list[dict[str, Any]] = []
    for index, run in enumerate(metal_runs, 1):
        if multimodal:
            pairs = [run]
            scores = [float(run.get("score", math.nan))]
            logits = [float(run.get("raw_logit", math.nan))]
            prompts = [run.get("rendered_prompt")]
            ids = [[int(token) for token in run.get("expanded_token_ids", [])]]
            mrope = [[int(value) for value in run.get("mrope_positions", [])]]
            expected_schema = "antfly.qwen3vl.multimodal_reranker_qualification.v1"
        else:
            pairs = run.get("pairs", [])
            scores = [float(value) for value in run.get("scores", [])]
            logits = [float(value) for value in run.get("raw_logits", [])]
            prompts = [pair.get("rendered_prompt") for pair in pairs if isinstance(pair, dict)]
            ids = [[int(token) for token in pair.get("token_ids", [])] for pair in pairs if isinstance(pair, dict)]
            mrope = []
            expected_schema = "antfly.qwen3vl.reranker_qualification.v1"
        score_abs = [abs(actual - reference) for actual, reference in zip(scores, oracle_scores)]
        logit_abs = [abs(actual - reference) for actual, reference in zip(logits, oracle_logits)]
        entry = {
            "repeat": index,
            "scores": scores,
            "raw_logits": logits,
            "score_abs": score_abs,
            "logit_abs": logit_abs,
            "ranking": stable_ranking(scores) if len(scores) == expected_count else [],
        }
        per_run.append(entry)
        add_gate(gates, f"metal_{index}_trace_schema", run.get("schema") == expected_schema, run.get("schema"))
        add_gate(gates, f"metal_{index}_backend", run.get("backend") == "metal", run.get("backend"))
        add_gate(gates, f"metal_{index}_output_count", len(scores) == expected_count and len(logits) == expected_count and len(pairs) == expected_count, {
            "scores": len(scores), "logits": len(logits), "pairs": len(pairs), "expected": expected_count
        })
        add_gate(gates, f"metal_{index}_prompt_exact", prompts == oracle_prompts, {
            "oracle": oracle_prompts, "metal": prompts
        })
        add_gate(gates, f"metal_{index}_token_ids_exact", ids == oracle_ids, {
            "oracle": oracle_ids, "metal": ids
        })
        if multimodal:
            visual_tokens = int(run.get("visual_tokens", -1))
            visual_mask = run.get("visual_token_mask", [])
            add_gate(gates, f"metal_{index}_typed_image_contract", run.get("images") == [str(args.image)], {
                "oracle": [str(args.image)], "metal": run.get("images")
            })
            add_gate(gates, f"metal_{index}_mrope_exact", mrope == oracle_mrope, {
                "oracle": oracle_mrope, "metal": mrope
            })
            add_gate(gates, f"metal_{index}_visual_token_count_exact", isinstance(oracle_image, dict) and visual_tokens == oracle_image.get("visual_tokens"), {
                "oracle": oracle_image.get("visual_tokens") if isinstance(oracle_image, dict) else None,
                "metal": visual_tokens,
            })
            add_gate(gates, f"metal_{index}_visual_mask_exact", isinstance(visual_mask, list) and len(visual_mask) == len(ids[0]) and sum(value is True for value in visual_mask) == visual_tokens, {
                "mask_length": len(visual_mask) if isinstance(visual_mask, list) else None,
                "token_length": len(ids[0]),
                "true_count": sum(value is True for value in visual_mask) if isinstance(visual_mask, list) else None,
                "visual_tokens": visual_tokens,
            })
        add_gate(gates, f"metal_{index}_values_finite", all(math.isfinite(value) for value in scores + logits), {
            "scores": scores, "logits": logits
        })
        add_gate(gates, f"metal_{index}_sigmoid_contract", len(scores) == len(logits) and all(
            abs(stable_sigmoid(logit) - score) <= 1e-6
            for logit, score in zip(logits, scores, strict=True)
        ), {"max_abs": max((abs(stable_sigmoid(logit) - score) for logit, score in zip(logits, scores)), default=0.0)})
        add_gate(gates, f"metal_{index}_score_parity", len(score_abs) == expected_count and max(score_abs, default=math.inf) <= limits["max_score_abs"], {
            "absolute": score_abs, "limit": limits["max_score_abs"]
        })
        add_gate(gates, f"metal_{index}_logit_parity", len(logit_abs) == expected_count and max(logit_abs, default=math.inf) <= limits["max_logit_abs"], {
            "absolute": logit_abs, "limit": limits["max_logit_abs"]
        })
        add_gate(gates, f"metal_{index}_ranking_exact", len(scores) == expected_count and stable_ranking(scores) == stable_ranking(oracle_scores), {
            "oracle": stable_ranking(oracle_scores), "metal": stable_ranking(scores) if len(scores) == expected_count else []
        })

    baseline = metal_runs[0]
    determinism = all(run == baseline for run in metal_runs[1:])
    add_gate(gates, "metal_cross_process_bitwise_determinism", determinism, {
        "repeat_count": len(metal_runs),
        "raw_logits": [run.get("raw_logits") for run in metal_runs],
        "scores": [run.get("scores") for run in metal_runs],
    })

    executions = [("oracle", oracle_execution)] + [
        (f"metal_{index}", execution) for index, execution in enumerate(metal_executions, 1)
    ]
    for name, execution in executions:
        resources = execution["resources"]
        max_rss = args.oracle_max_rss_mib if name == "oracle" else args.metal_max_rss_mib
        add_gate(gates, f"{name}_rss_within_limit", resources["max_rss_mib"] <= max_rss, {
            "actual_mib": resources["max_rss_mib"], "limit_mib": max_rss
        })
        add_gate(gates, f"{name}_free_memory_within_limit", resources["min_free_percent"] >= args.min_free_percent, {
            "actual_percent": resources["min_free_percent"], "limit_percent": args.min_free_percent
        })
        add_gate(gates, f"{name}_swap_growth_within_limit", resources["swapout_growth_mib"] <= args.max_swap_growth_mib, {
            "actual_mib": resources["swapout_growth_mib"], "limit_mib": args.max_swap_growth_mib
        })
        combined_output = execution["stdout"] + "\n" + execution["stderr"]
        forbidden = FORBIDDEN_RUNTIME_OUTPUT.search(combined_output)
        add_gate(gates, f"{name}_no_forbidden_runtime_output", forbidden is None, forbidden.group(0) if forbidden else None)

    return all(gate["pass"] for gate in gates), gates, {
        "limits": limits,
        "decoder_quantization": args.decoder_quantization,
        "calibration_profile": args.profile,
        "oracle_scores": oracle_scores,
        "oracle_logits": oracle_logits,
        "oracle_ranking": stable_ranking(oracle_scores),
        "metal": per_run,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.system() != "Darwin":
        raise QualificationError("real Qwen3-VL reranker Metal qualification requires macOS")
    args.model_dir = args.model_dir.resolve(strict=True)
    args.oracle_model_dir = args.oracle_model_dir.resolve(strict=True)
    args.antfly_bin = args.antfly_bin.resolve(strict=True)
    args.oracle_script = args.oracle_script.resolve(strict=True)
    if args.image is not None:
        args.image = args.image.resolve(strict=True)
    work_dir = args.artifacts_dir or args.output.with_suffix(".artifacts")
    work_dir.mkdir(parents=True, exist_ok=False)
    bundle = validate_published_bundle(args.model_dir, args.decoder_quantization)
    oracle_bundle = validate_managed_reranker_bundle(args.oracle_model_dir)
    oracle, oracle_execution = run_oracle(args, work_dir)
    metal_runs: list[dict[str, Any]] = []
    metal_executions: list[dict[str, Any]] = []
    for repeat in range(1, args.metal_repeat_count + 1):
        evidence, execution = run_metal(args, work_dir, repeat)
        metal_runs.append(evidence)
        metal_executions.append(execution)
    passed, gates, comparison = evaluate(
        args,
        oracle,
        metal_runs,
        oracle_execution,
        metal_executions,
    )
    return {
        "schema": SCHEMA,
        "pass": passed,
        "release_ready": False,
        "scope": (
            "bf16_multimodal_reranker_transformers_and_quantized_bounded_metal_acceptance"
            if args.image is not None
            else "bf16_text_reranker_transformers_and_quantized_bounded_metal_acceptance"
        ),
        "created_unix_seconds": int(time.time()),
        "host": {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "work_dir": str(work_dir.resolve()),
        "model": bundle,
        "oracle_model": oracle_bundle,
        "binary": git_provenance(args.antfly_bin),
        "fixture": {
            "instruction": args.instruction,
            "query": args.query,
            "documents": args.document,
            "image": str(args.image) if args.image is not None else None,
        },
        "oracle": oracle,
        "oracle_execution": oracle_execution,
        "metal_runs": metal_runs,
        "metal_executions": metal_executions,
        "comparison": comparison,
        "gates": gates,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    directory = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--oracle-model-dir", type=Path, required=True)
    parser.add_argument("--antfly-bin", type=Path, required=True)
    parser.add_argument("--oracle-script", type=Path, default=directory / "transformers_reranker_oracle.py")
    parser.add_argument("--instruction", default="Given a search query, retrieve relevant candidates that answer the query.")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--document", action="append")
    parser.add_argument("--image", type=Path)
    parser.add_argument(
        "--decoder-quantization",
        choices=tuple(SCORE_LIMITS),
        default="Q8_0",
    )
    parser.add_argument(
        "--profile",
        choices=("calibrated", "ranking-only"),
        default="calibrated",
    )
    parser.add_argument("--metal-repeat-count", type=int, default=2)
    parser.add_argument("--oracle-threads", type=int, default=4)
    parser.add_argument("--oracle-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--metal-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--oracle-max-rss-mib", type=float, default=7_000.0)
    parser.add_argument("--metal-max-rss-mib", type=float, default=7_000.0)
    parser.add_argument("--min-free-percent", type=int, default=10)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.1)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.document is None:
        args.document = list(DEFAULT_DOCUMENTS)
    if args.image is not None and len(args.document) != 1:
        parser.error("--image requires exactly one --document")
    if args.profile == "calibrated" and args.decoder_quantization != "Q8_0":
        parser.error("the calibrated profile requires --decoder-quantization Q8_0")
    if args.profile == "ranking-only" and args.decoder_quantization != "Q4_K_M":
        parser.error("the ranking-only profile requires --decoder-quantization Q4_K_M")
    if not 2 <= args.metal_repeat_count <= 5:
        parser.error("--metal-repeat-count must be in [2, 5]")
    if not 1 <= args.oracle_threads <= 16:
        parser.error("--oracle-threads must be in [1, 16]")
    if not 1 <= args.min_free_percent <= 100:
        parser.error("--min-free-percent must be in [1, 100]")
    if args.oracle_max_rss_mib <= 0 or args.metal_max_rss_mib <= 0 or args.max_swap_growth_mib < 0:
        parser.error("resource limits must be positive and swap growth must be non-negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        report = run(args)
    except (QualificationError, OSError, RuntimeError, ValueError) as exc:
        report = {
            "schema": SCHEMA,
            "pass": False,
            "release_ready": False,
            "created_unix_seconds": int(time.time()),
            "host": {"platform": platform.platform(), "python": platform.python_version()},
            "error": str(exc),
        }
        write_json_atomic(args.output, report)
        print(f"Qwen3-VL reranker Metal qualification failed: {exc}", file=sys.stderr)
        return 2
    write_json_atomic(args.output, report)
    if not report["pass"]:
        print("Qwen3-VL reranker Metal qualification gates failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
