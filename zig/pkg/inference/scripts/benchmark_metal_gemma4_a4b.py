#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed paired benchmark and release gate for Gemma4 26B-A4B Metal.

Every sample runs in a fresh process. Measured samples require exact token
parity, the qualified A4B runtime marker, zero routed-linear/scatter fallback,
paired gate/up dispatch, a bounded maximum RSS, and immutable provenance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from gemma4_metal_long_output import (  # noqa: E402
    BenchmarkContractError,
    _file_sha256,
    _mapping,
    _positive_finite,
    _token_line,
)


METADATA_SCHEMA = "antfly.gemma4_a4b_metal_ab.metadata.v1"
SUMMARY_SCHEMA = "antfly.gemma4_a4b_metal_ab.summary.v1"
POLICY_ENV_PREFIXES = ("TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_")
HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
A4B_RUNTIME = re.compile(
    r"^(?:info:\s+)?gemma4_a4b_runtime:\s+mode=(auto|streamed|resident)\s+"
    r"budget_mb=(\d+)\s+kv_mb=(\d+)\s+safety_mb=(\d+)\s+"
    r"expert_slots=(\d+)\b",
    re.MULTILINE,
)
MAX_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _git_provenance(repo: Path) -> dict[str, Any]:
    env = {**os.environ, "LC_ALL": "C"}
    revision = subprocess.check_output(
        ("git", "-C", str(repo), "rev-parse", "HEAD"), env=env
    ).decode().strip()
    status = subprocess.check_output(
        ("git", "-C", str(repo), "status", "--porcelain=v1", "--untracked-files=all"),
        env=env,
    )
    diff = subprocess.check_output(
        ("git", "-C", str(repo), "diff", "--binary", "--no-ext-diff", "HEAD", "--"),
        env=env,
    )
    return {
        "git_revision": revision,
        "git_dirty": bool(status.rstrip(b"\n")),
        "git_status_sha256": hashlib.sha256(status).hexdigest(),
        "git_tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
    }


def _resolve_gguf(model: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        result = explicit.resolve()
    elif model.is_file():
        result = model.resolve()
    else:
        candidates = [
            path
            for path in model.glob("*.gguf")
            if "mmproj" not in path.name.lower() and "projector" not in path.name.lower()
        ]
        if not candidates:
            raise BenchmarkContractError(f"no decoder GGUF found under {model}")
        if len(candidates) != 1:
            rendered = ", ".join(str(path.resolve()) for path in sorted(candidates))
            raise BenchmarkContractError(
                f"model directory must contain exactly one decoder GGUF; found {rendered}"
            )
        result = candidates[0].resolve()
    if not result.is_file():
        raise BenchmarkContractError(f"GGUF does not exist: {result}")
    return result


def _metric_stats(values: list[float]) -> dict[str, float]:
    if not values:
        raise BenchmarkContractError("cannot summarize an empty sample set")
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": mean,
        "max": max(values),
        "stdev": stdev,
        "cv": stdev / mean if mean else math.inf,
    }


def _exact_int(mapping: dict[str, Any], key: str, label: str, path: Path) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BenchmarkContractError(f"{label}.{key} must be an integer: {path}")
    return value


def _last_runtime_marker(log: str, path: Path) -> tuple[str, int, int, int, int]:
    matches = list(A4B_RUNTIME.finditer(log))
    if not matches:
        raise BenchmarkContractError(f"qualified A4B runtime marker missing: {path}")
    match = matches[-1]
    return (
        match.group(1),
        int(match.group(2)),
        int(match.group(3)),
        int(match.group(4)),
        int(match.group(5)),
    )


def parse_sample(
    json_path: Path,
    log_path: Path,
    *,
    expected_mode: str,
    expected_budget_mb: int,
    expected_device: str,
    expected_prompt_tokens: int,
    expected_prompt_sha256: str,
    expected_output_tokens: int,
    expected_output_sha256: str,
    max_rss_mb: float,
) -> dict[str, Any]:
    try:
        payload = json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"invalid Antfly JSON {json_path}: {exc}") from exc
    log = log_path.read_text(errors="replace")
    if payload.get("backend") != "metal":
        raise BenchmarkContractError(f"sample did not report Metal: {json_path}")
    if payload.get("tokens") != expected_output_tokens or payload.get("finish_reason") != "length":
        raise BenchmarkContractError(
            f"sample was not exactly {expected_output_tokens} length-limited tokens: {json_path}"
        )

    output_text, output_ids = _token_line(log, "token_ids", log_path)
    prompt_text, prompt_ids = _token_line(log, "prompt_token_ids", log_path)
    output_sha = _sha256_text(output_text)
    prompt_sha = _sha256_text(prompt_text)
    if len(output_ids) != expected_output_tokens or output_sha != expected_output_sha256:
        raise BenchmarkContractError(
            f"output token contract mismatch count={len(output_ids)} sha={output_sha}: {log_path}"
        )
    if len(prompt_ids) != expected_prompt_tokens or prompt_sha != expected_prompt_sha256:
        raise BenchmarkContractError(
            f"prompt token contract mismatch count={len(prompt_ids)} sha={prompt_sha}: {log_path}"
        )
    if payload.get("token_ids") is not None and payload["token_ids"] != output_ids:
        raise BenchmarkContractError(f"JSON/log output token IDs differ: {json_path}")

    mode, budget_mb, kv_mb, safety_mb, expert_slots = _last_runtime_marker(log, log_path)
    if mode != expected_mode or budget_mb != expected_budget_mb:
        raise BenchmarkContractError(
            f"A4B runtime mode/budget={mode}/{budget_mb}, expected "
            f"{expected_mode}/{expected_budget_mb}: {log_path}"
        )
    if kv_mb <= 0 or safety_mb <= 0 or expert_slots < 8:
        raise BenchmarkContractError(f"invalid A4B bounded runtime geometry: {log_path}")

    metal = _mapping(payload.get("metal"), "metal", json_path)
    if metal.get("device") != expected_device:
        raise BenchmarkContractError(
            f"Metal device={metal.get('device')!r}, expected {expected_device!r}: {json_path}"
        )
    residency = _mapping(metal.get("residency"), "metal.residency", json_path)
    counters = {
        key: _exact_int(residency, key, "metal.residency", json_path)
        for key in (
            "runtime_mapped_fallbacks",
            "runtime_mapped_failures",
            "a4b_linear_attempts",
            "a4b_linear_successes",
            "a4b_linear_fallbacks",
            "a4b_pair_attempts",
            "a4b_pair_successes",
            "a4b_pair_fallbacks",
            "a4b_scatter_attempts",
            "a4b_scatter_successes",
            "a4b_scatter_fallbacks",
        )
    }
    if counters["runtime_mapped_fallbacks"] or counters["runtime_mapped_failures"]:
        raise BenchmarkContractError(f"mapped expert preparation fallback/failure: {json_path}")
    for prefix in ("a4b_linear", "a4b_pair", "a4b_scatter"):
        attempts = counters[f"{prefix}_attempts"]
        successes = counters[f"{prefix}_successes"]
        fallbacks = counters[f"{prefix}_fallbacks"]
        if attempts <= 0 or attempts != successes or fallbacks != 0:
            raise BenchmarkContractError(
                f"{prefix} attempts/successes/fallbacks={attempts}/{successes}/{fallbacks}: "
                f"{json_path}"
            )

    timing = _mapping(payload.get("timing_ms"), "timing_ms", json_path)
    total_ms = _positive_finite(float(timing.get("generate") or 0), "generate", json_path)
    prefill_ms = _positive_finite(
        float(timing.get("prefill_inner") or 0), "prefill_inner", json_path
    )
    decode_ms = _positive_finite(
        float(timing.get("decode_inner") or 0), "decode_inner", json_path
    )
    if abs(prefill_ms + decode_ms - total_ms) > max(2.0, total_ms * 0.001):
        raise BenchmarkContractError(f"phase timings do not reconcile: {json_path}")

    rss_match = MAX_RSS.search(log)
    if rss_match is None:
        raise BenchmarkContractError(f"macOS maximum RSS marker missing: {log_path}")
    max_rss = int(rss_match.group(1)) / (1024 * 1024)
    if max_rss_mb > 0 and max_rss > max_rss_mb:
        raise BenchmarkContractError(
            f"maximum RSS {max_rss:.1f} MiB exceeds {max_rss_mb:.1f} MiB: {log_path}"
        )
    decode_frames = max(expected_output_tokens - 1, 1)
    return {
        "mode": mode,
        "memory_budget_mb": budget_mb,
        "expert_slots": expert_slots,
        "prompt_tokens": len(prompt_ids),
        "output_tokens": len(output_ids),
        "prompt_token_ids_sha256": prompt_sha,
        "output_token_ids_sha256": output_sha,
        "total_ms": total_ms,
        "prefill_ms": prefill_ms,
        "decode_ms": decode_ms,
        "decode_tok_s": decode_frames * 1000.0 / decode_ms,
        "max_rss_mb": max_rss,
        "routes": counters,
    }


def _effective_environment(explicit: dict[str, str]) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(POLICY_ENV_PREFIXES)
    }
    environment.update(explicit)
    environment["TERMITE_GEN_STAGE_DEBUG"] = "1"
    environment["ANTFLY_INFERENCE_JSON_TOKEN_IDS"] = "1"
    return environment


def _run_sample(
    *,
    binary: Path,
    model: Path,
    prompt: str,
    execution_mode: str,
    mode: str,
    budget_mb: int,
    output_tokens: int,
    json_path: Path,
    log_path: Path,
    environment: dict[str, str],
) -> None:
    # Both inputs are complete Antfly CLI builds; their filenames may differ
    # after being copied into immutable benchmark artifact directories.
    command = ["/usr/bin/time", "-l", str(binary), "inference"]
    command.extend(
        (
            "generate",
            str(model),
            prompt,
            "--backend",
            "metal",
            "--mode",
            execution_mode,
            "--a4b-residency-mode",
            mode,
            "--a4b-memory-budget-mb",
            str(budget_mb),
            "--max-tokens",
            str(output_tokens),
            "--temperature",
            "0",
            "--raw-prompt",
            "--ignore-eos",
            "--cache-dtype",
            "f16",
            "--print-token-count",
            "--print-finish-reason",
            "--print-token-ids",
            "--print-prompt-token-ids",
            "--print-timing",
            "--json-timing",
            str(json_path),
        )
    )
    if execution_mode == "compiled":
        command[command.index("--a4b-residency-mode"):command.index("--a4b-residency-mode")] = (
            "--compiled-target",
            "whole-model",
        )
    with log_path.open("w") as log_file:
        completed = subprocess.run(
            command,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise BenchmarkContractError(
            f"sample exited {completed.returncode}; see {log_path}"
        )


def _validate_sha(value: str, label: str) -> str:
    normalized = value.lower()
    if HEX_SHA256.fullmatch(normalized) is None:
        raise BenchmarkContractError(f"{label} must be a SHA-256")
    return normalized


def build_summary(root: Path) -> dict[str, Any]:
    metadata_path = root / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("schema") != METADATA_SCHEMA:
        raise BenchmarkContractError(f"unsupported metadata schema: {metadata_path}")

    samples: list[dict[str, Any]] = []
    for invocation in metadata["invocations"]:
        parsed = parse_sample(
            root / f"{invocation['label']}.json",
            root / f"{invocation['label']}.log",
            expected_mode=metadata[f"{invocation['variant']}_mode"],
            expected_budget_mb=metadata[f"{invocation['variant']}_budget_mb"],
            expected_device=metadata["expected_metal_device"],
            expected_prompt_tokens=metadata["expected_prompt_tokens"],
            expected_prompt_sha256=metadata["expected_prompt_token_ids_sha256"],
            expected_output_tokens=metadata["output_tokens"],
            expected_output_sha256=metadata["expected_output_token_ids_sha256"],
            max_rss_mb=metadata[f"{invocation['variant']}_max_rss_mb"],
        )
        samples.append({**invocation, **parsed})

    measured = [sample for sample in samples if not sample["warmup"]]
    by_pair: dict[int, dict[str, dict[str, Any]]] = {}
    for sample in measured:
        by_pair.setdefault(sample["pair"], {})[sample["variant"]] = sample
    pairs: list[dict[str, Any]] = []
    for pair_index in sorted(by_pair):
        pair = by_pair[pair_index]
        if set(pair) != {"baseline", "candidate"}:
            raise BenchmarkContractError(f"incomplete measured pair {pair_index}")
        baseline = pair["baseline"]
        candidate = pair["candidate"]
        pairs.append(
            {
                "pair": pair_index,
                "candidate_total_latency_ratio": candidate["total_ms"]
                / baseline["total_ms"],
                "candidate_prefill_latency_ratio": candidate["prefill_ms"]
                / baseline["prefill_ms"],
                "candidate_decode_latency_ratio": candidate["decode_ms"]
                / baseline["decode_ms"],
                "candidate_decode_throughput_ratio": candidate["decode_tok_s"]
                / baseline["decode_tok_s"],
                "baseline_max_rss_mb": baseline["max_rss_mb"],
                "candidate_max_rss_mb": candidate["max_rss_mb"],
            }
        )

    variant_metrics: dict[str, Any] = {}
    failures: list[str] = []
    for variant in ("baseline", "candidate"):
        variant_samples = [sample for sample in measured if sample["variant"] == variant]
        variant_metrics[variant] = {
            metric: _metric_stats([float(sample[metric]) for sample in variant_samples])
            for metric in (
                "total_ms",
                "prefill_ms",
                "decode_ms",
                "decode_tok_s",
                "max_rss_mb",
            )
        }
        for metric in ("total_ms", "prefill_ms", "decode_ms", "decode_tok_s"):
            cv = variant_metrics[variant][metric]["cv"]
            if cv > metadata["max_cv"]:
                failures.append(f"{variant} {metric} CV {cv:.4f} exceeds {metadata['max_cv']:.4f}")

    ratio_stats = {
        key: _metric_stats([float(pair[key]) for pair in pairs])
        for key in (
            "candidate_total_latency_ratio",
            "candidate_prefill_latency_ratio",
            "candidate_decode_latency_ratio",
            "candidate_decode_throughput_ratio",
        )
    }
    checks = {
        "total_latency": ratio_stats["candidate_total_latency_ratio"]["median"]
        <= metadata["max_total_latency_ratio"],
        "decode_latency": ratio_stats["candidate_decode_latency_ratio"]["median"]
        <= metadata["max_decode_latency_ratio"],
        "decode_throughput": ratio_stats["candidate_decode_throughput_ratio"]["median"]
        >= metadata["min_decode_throughput_ratio"],
        "target_wins": sum(
            pair["candidate_total_latency_ratio"] < 1.0 for pair in pairs
        )
        >= metadata["min_target_wins"],
        "cv": not failures,
    }
    failures.extend(name for name, passed in checks.items() if not passed and name != "cv")
    return {
        "schema": SUMMARY_SCHEMA,
        "experiment_id": metadata["experiment_id"],
        "metadata": metadata,
        "samples": samples,
        "pairs": pairs,
        "metrics": variant_metrics,
        "paired_ratios": ratio_stats,
        "checks": checks,
        "failures": failures,
        "passed": not failures,
    }


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    if args.runs < 2:
        raise BenchmarkContractError("--runs must be at least two paired runs")
    if args.warmups < 0 or args.cooldown_seconds < 0:
        raise BenchmarkContractError("warmups and cooldown must be non-negative")
    if not 0 < args.max_cv < 1:
        raise BenchmarkContractError("--max-cv must be between zero and one")
    if not 0 <= args.min_target_wins <= args.runs:
        raise BenchmarkContractError("--min-target-wins must be between zero and --runs")
    expected_prompt_sha = _validate_sha(
        args.expected_prompt_token_ids_sha256, "--expected-prompt-token-ids-sha256"
    )
    expected_output_sha = _validate_sha(
        args.expected_output_token_ids_sha256, "--expected-output-token-ids-sha256"
    )

    root = args.out_dir.resolve()
    if root.exists() and any(root.iterdir()):
        raise BenchmarkContractError(f"--out-dir must be empty: {root}")
    model = args.model.resolve()
    if not model.exists():
        raise BenchmarkContractError(f"model does not exist: {model}")
    gguf = _resolve_gguf(model, args.gguf)
    baseline_bin = args.baseline_bin.resolve()
    candidate_bin = args.candidate_bin.resolve()
    for binary in (baseline_bin, candidate_bin):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise BenchmarkContractError(f"binary is not executable: {binary}")

    invocations: list[dict[str, Any]] = []
    serial = 0
    for warmup in range(args.warmups):
        for variant in ("baseline", "candidate"):
            invocations.append(
                {
                    "label": f"{serial:03d}-warmup-{warmup}-{variant}",
                    "variant": variant,
                    "pair": warmup,
                    "warmup": True,
                }
            )
            serial += 1
    for pair in range(args.runs):
        order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
        for variant in order:
            invocations.append(
                {
                    "label": f"{serial:03d}-pair-{pair}-{variant}",
                    "variant": variant,
                    "pair": pair,
                    "warmup": False,
                }
            )
            serial += 1

    repo = SCRIPT_DIR.parents[3]
    prompt = args.prompt
    metadata = {
        "schema": METADATA_SCHEMA,
        "experiment_id": args.experiment_id,
        "repo_root": str(repo),
        **_git_provenance(repo),
        "runner_sha256": _file_sha256(Path(__file__).resolve()),
        "host": platform.platform(),
        "machine": platform.machine(),
        "expected_metal_device": args.expected_metal_device,
        "execution_mode": args.execution_mode,
        "model": str(model),
        "gguf": str(gguf),
        "gguf_sha256": _file_sha256(gguf),
        "baseline_bin": str(baseline_bin),
        "baseline_bin_sha256": _file_sha256(baseline_bin),
        "candidate_bin": str(candidate_bin),
        "candidate_bin_sha256": _file_sha256(candidate_bin),
        "prompt_sha256": _sha256_text(prompt),
        "expected_prompt_tokens": args.expected_prompt_tokens,
        "expected_prompt_token_ids_sha256": expected_prompt_sha,
        "output_tokens": args.output_tokens,
        "expected_output_token_ids_sha256": expected_output_sha,
        "baseline_mode": args.baseline_mode,
        "baseline_budget_mb": args.baseline_budget_mb,
        "baseline_max_rss_mb": args.baseline_max_rss_mb,
        "candidate_mode": args.candidate_mode,
        "candidate_budget_mb": args.candidate_budget_mb,
        "candidate_max_rss_mb": args.candidate_max_rss_mb,
        "runs": args.runs,
        "warmups": args.warmups,
        "cooldown_seconds": args.cooldown_seconds,
        "max_total_latency_ratio": args.max_total_latency_ratio,
        "max_decode_latency_ratio": args.max_decode_latency_ratio,
        "min_decode_throughput_ratio": args.min_decode_throughput_ratio,
        "min_target_wins": args.min_target_wins,
        "max_cv": args.max_cv,
        "invocations": invocations,
        "environment": {
            "policy_prefixes_cleared": list(POLICY_ENV_PREFIXES),
            "explicit": dict(sorted(args.environment.items())),
            "runner_owned": {
                "TERMITE_GEN_STAGE_DEBUG": "1",
                "ANTFLY_INFERENCE_JSON_TOKEN_IDS": "1",
            },
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / "prompt.txt").write_text(prompt)
    _write_json(root / "metadata.json", metadata)
    environment = _effective_environment(args.environment)
    for invocation in invocations:
        variant = invocation["variant"]
        _run_sample(
            binary=baseline_bin if variant == "baseline" else candidate_bin,
            model=model,
            prompt=prompt,
            execution_mode=args.execution_mode,
            mode=metadata[f"{variant}_mode"],
            budget_mb=metadata[f"{variant}_budget_mb"],
            output_tokens=args.output_tokens,
            json_path=root / f"{invocation['label']}.json",
            log_path=root / f"{invocation['label']}.log",
            environment=environment,
        )
        if args.cooldown_seconds:
            time.sleep(args.cooldown_seconds)
    summary = build_summary(root)
    _write_json(root / "summary.json", summary)
    return summary


def _env_assignment(value: str) -> tuple[str, str]:
    name, separator, env_value = value.partition("=")
    if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise argparse.ArgumentTypeError("environment values must be NAME=VALUE")
    return name, env_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--out-dir", type=Path, required=True)
    run.add_argument("--experiment-id", required=True)
    run.add_argument("--model", type=Path, required=True)
    run.add_argument("--gguf", type=Path)
    run.add_argument("--baseline-bin", type=Path, required=True)
    run.add_argument("--candidate-bin", type=Path, required=True)
    run.add_argument("--prompt", required=True)
    run.add_argument("--execution-mode", choices=("eager", "compiled"), default="eager")
    run.add_argument("--expected-prompt-tokens", type=int, required=True)
    run.add_argument("--expected-prompt-token-ids-sha256", required=True)
    run.add_argument("--output-tokens", type=int, default=128)
    run.add_argument("--expected-output-token-ids-sha256", required=True)
    run.add_argument("--baseline-mode", choices=("streamed", "resident"), default="streamed")
    run.add_argument("--baseline-budget-mb", type=int, default=2048)
    run.add_argument("--baseline-max-rss-mb", type=float, default=2304)
    run.add_argument("--candidate-mode", choices=("streamed", "resident"), default="streamed")
    run.add_argument("--candidate-budget-mb", type=int, default=2048)
    run.add_argument("--candidate-max-rss-mb", type=float, default=2304)
    run.add_argument("--runs", type=int, default=6)
    run.add_argument("--warmups", type=int, default=1)
    run.add_argument("--cooldown-seconds", type=int, default=15)
    run.add_argument("--expected-metal-device", default="Apple M4")
    run.add_argument("--max-total-latency-ratio", type=float, default=0.95)
    run.add_argument("--max-decode-latency-ratio", type=float, default=0.95)
    run.add_argument("--min-decode-throughput-ratio", type=float, default=1.05)
    run.add_argument("--min-target-wins", type=int, default=5)
    run.add_argument("--max-cv", type=float, default=0.05)
    run.add_argument("--env", action="append", type=_env_assignment, default=[])

    summarize = subparsers.add_parser("summarize")
    summarize.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run":
        args.environment = dict(args.env)
    return args


def main() -> None:
    args = parse_args()
    try:
        if args.command == "run":
            summary = run_experiment(args)
        else:
            summary = build_summary(args.out_dir.resolve())
            _write_json(args.out_dir.resolve() / "summary.json", summary)
    except (BenchmarkContractError, OSError, subprocess.SubprocessError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    ratios = summary["paired_ratios"]
    print(
        f"Gemma4 A4B Metal {summary['experiment_id']}: "
        f"total={ratios['candidate_total_latency_ratio']['median']:.4f} "
        f"decode={ratios['candidate_decode_latency_ratio']['median']:.4f} "
        f"throughput={ratios['candidate_decode_throughput_ratio']['median']:.4f} "
        f"passed={summary['passed']}"
    )


if __name__ == "__main__":
    main()
