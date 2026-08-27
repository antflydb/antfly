#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Run and summarize fail-closed Gemma4 Metal baseline/candidate experiments.

This deliberately has a separate schema from the pinned Antfly/llama.cpp gate.
Every invocation is a fresh process because Metal policy environment variables
are cached by the runtime. Optional stage-timing runs are diagnostic artifacts;
they are never included in performance medians, ratios, CVs, or win counts.
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
import subprocess
import sys
import time
from typing import Any, Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from gemma4_metal_long_output import (  # noqa: E402
    BenchmarkContractError,
    _exact_int,
    _file_sha256,
    _last_match,
    _mapping,
    _positive_finite,
    _token_line,
    stats,
)


METADATA_SCHEMA = "antfly.gemma4_metal_ab.metadata.v3"
SUMMARY_SCHEMA = "antfly.gemma4_metal_ab.v3"
SHARED_PARSER = SCRIPT_DIR / "gemma4_metal_long_output.py"
ROUTE_PROFILES = (
    "split_ffn",
    "q4_mmv_workload",
    "pair_decode",
    "pair_prefill",
    "pair_decode_prefill",
    "concurrent_split",
    "gqa_split_schedule",
)
GQA_SPLIT_VARIANTS = ("s8", "s16", "s24", "s32")
GQA_SPLIT_VARIANT_ENV = {
    "swa": "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
    "global": "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
}
GQA_SPLIT_TRACE_ENV = "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE"
Q4_MMV_VARIANTS = ("nr4-nsg2", "nr8-nsg2", "nr4-nsg4", "nr8-nsg4")
Q4_MMV_TRACE_ENV = "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT"
Q4_MMV_WORKLOAD_ENV = {
    "attention": "TERMITE_METAL_Q4_0_MMV_ATTENTION_VARIANT",
    "ffn_gate_up": "TERMITE_METAL_Q4_0_MMV_FFN_GATE_UP_VARIANT",
    "ffn_down": "TERMITE_METAL_Q4_0_MMV_FFN_DOWN_VARIANT",
    "ple": "TERMITE_METAL_Q4_0_MMV_PLE_VARIANT",
    "tail": "TERMITE_METAL_Q4_0_MMV_TAIL_VARIANT",
}
# Exact Gemma4 E4B Q4 row-one dispatch topology for one decode frame.  Keep
# this independent of tensor shape: the generic provider boundary and tagged
# attention projection currently share 2560x2048, but have different semantic
# ownership and dispatch counts.
Q4_MMV_WORKLOAD_DISPATCHES_PER_FRAME = {
    "generic": 18,
    "attention": 66,
    "ffn_gate_up": 84,
    "ffn_down": 42,
}
GQA_SPLIT_SCHEDULE_KEYS = (
    "legacy_total",
    "swa_total",
    "global_total",
    *(f"swa_{variant}" for variant in GQA_SPLIT_VARIANTS),
    *(f"global_{variant}" for variant in GQA_SPLIT_VARIANTS),
    "fallbacks",
    "invalid_overrides",
)
PAIR_POLICY_KEYS = (
    "mmv_nr4_nsg2",
    "mmv_nr8_nsg2",
    "mmv_nr4_nsg4",
    "mmv_nr8_nsg4",
    "mmv_variant_fallbacks",
    "mm_m32_n64_aligned",
    "mm_m32_n64_tail",
    "mm_m32_n32_aligned",
    "mm_m32_n32_tail",
    "mm_variant_fallbacks",
)
STAGE_NAMES = ("attention", "ffn", "ple", "tail", "embedding", "other")
STAGE_TIMING_KEYS = (
    "enabled",
    "supported",
    "complete",
    "prefill_frames",
    "prefill_gpu",
    *(f"prefill_{name}" for name in STAGE_NAMES),
    "decode_frames",
    "decode_gpu",
    *(f"decode_{name}" for name in STAGE_NAMES),
    "samples",
    "failures",
)
CONTROLLED_ENV_NAMES = frozenset(
    {
        "TERMITE_METAL_STAGE_TIMING",
        "TERMITE_METAL_STAGE_TIMING_PREFILL_MAX",
        "TERMITE_METAL_STAGE_TIMING_DECODE_START",
        "TERMITE_METAL_STAGE_TIMING_DECODE_STRIDE",
        "TERMITE_METAL_STAGE_TIMING_DECODE_MAX",
        "TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT",
        "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT",
        "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
        "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
        "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
        "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
        "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
        "TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION",
        "TERMITE_METAL_DISABLE_PREFILL_SG_ATTENTION",
        "TERMITE_METAL_ENABLE_FLASH_PREFILL_GENERATED",
        "TERMITE_METAL_DISABLE_FLASH_PREFILL_GENERATED",
        "TERMITE_METAL_DISABLE_PREFILL_FLASH_HD512",
        "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP",
        "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME",
        "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS",
        "TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME",
        "TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME",
        "TERMITE_METAL_DISABLE_PLANNED_COMPUTE_BARRIERS",
        "TERMITE_METAL_DISABLE_PLANNED_ENCODER_COALESCING",
        "TERMITE_METAL_Q4_0_MMV_VARIANT",
        *Q4_MMV_WORKLOAD_ENV.values(),
        "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO",
        Q4_MMV_TRACE_ENV,
        "TERMITE_METAL_ENABLE_Q4_0_MM_SG_ALIGNED",
        "TERMITE_METAL_DISABLE_Q4_0_MM_SG_ALIGNED",
        "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION",
        "TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION",
        "TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MMV_VARIANT",
        "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM",
        "TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_MM",
        "TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MM_VARIANT",
        "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_SMALL_BATCH",
        "TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_SMALL_BATCH",
        "TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE",
        "TERMITE_METAL_ENABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
        "TERMITE_METAL_DISABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
        "TERMITE_METAL_ENABLE_RMS_NORM_GENERATED",
        "TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH",
        "TERMITE_METAL_DISABLE_CONCURRENT_PLANNED_DISPATCH",
    }
)
POLICY_ENV_PREFIXES = ("TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_")
RUNNER_OWNED_ENV_NAMES = frozenset(
    {
        "TERMITE_GEN_STAGE_DEBUG",
        "TERMITE_METAL_STAGE_TIMING",
        GQA_SPLIT_TRACE_ENV,
        Q4_MMV_TRACE_ENV,
    }
)
STAGE_TIMING_SAMPLING = {
    "prefill_max": 1,
    "decode_start": 32,
    "decode_stride": 64,
    "decode_max": 5,
}
STAGE_TIMING_SCOPE = "runtime_frame"
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _run_git(repo: Path, *args: str) -> bytes:
    return subprocess.check_output(
        ("git", "-C", str(repo), *args),
        env={**os.environ, "LC_ALL": "C"},
    )


def _git_provenance(repo: Path) -> dict[str, Any]:
    revision = _run_git(repo, "rev-parse", "HEAD").decode().strip()
    status = _run_git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    tracked_diff = _run_git(repo, "diff", "--binary", "--no-ext-diff", "HEAD", "--")
    return {
        "git_revision": revision,
        "git_dirty": bool(status.rstrip(b"\n")),
        "git_status_sha256": hashlib.sha256(status).hexdigest(),
        "git_tracked_diff_sha256": hashlib.sha256(tracked_diff).hexdigest(),
    }


def _resolve_gguf(model: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        result = explicit.resolve()
    elif model.is_file():
        result = model.resolve()
    else:
        matches = sorted(
            path.resolve()
            for path in model.glob("*.gguf")
            if "mmproj" not in path.name.lower()
        )
        if not matches:
            raise BenchmarkContractError(f"no text GGUF found under model path: {model}")
        result = matches[0]
    if not result.is_file():
        raise BenchmarkContractError(f"GGUF is not a file: {result}")
    return result


def _parse_env_entries(entries: Iterable[str], label: str) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for entry in entries:
        name, separator, value = entry.partition("=")
        if _ENV_NAME.fullmatch(name) is None:
            raise BenchmarkContractError(f"invalid {label} environment name: {name!r}")
        if name in result:
            raise BenchmarkContractError(f"duplicate {label} environment name: {name}")
        if "\n" in value or "\r" in value or "\0" in value:
            raise BenchmarkContractError(f"control character in {label} environment value: {name}")
        result[name] = value if separator else None
    return result


def _merge_env_json(entries: list[str], raw_json: str, label: str) -> dict[str, str | None]:
    result = _parse_env_entries(entries, label)
    if not raw_json.strip():
        return result
    try:
        decoded = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise BenchmarkContractError(f"invalid {label} JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise BenchmarkContractError(f"{label} JSON must be an object")
    rendered: list[str] = []
    for name, value in decoded.items():
        if not isinstance(name, str) or (value is not None and not isinstance(value, str)):
            raise BenchmarkContractError(f"{label} JSON values must be strings or null")
        rendered.append(name if value is None else f"{name}={value}")
    extra = _parse_env_entries(rendered, label)
    overlap = sorted(result.keys() & extra.keys())
    if overlap:
        raise BenchmarkContractError(f"duplicate {label} environment names: {', '.join(overlap)}")
    result.update(extra)
    return result


def _validate_variant_environments(
    common: dict[str, str | None],
    baseline: dict[str, str | None],
    candidate: dict[str, str | None],
    baseline_profile: str,
    candidate_profile: str,
) -> None:
    if common.keys() & (baseline.keys() | candidate.keys()):
        overlap = sorted(common.keys() & (baseline.keys() | candidate.keys()))
        raise BenchmarkContractError(
            f"common and variant environment maps overlap: {', '.join(overlap)}"
        )
    for runner_owned in RUNNER_OWNED_ENV_NAMES:
        if runner_owned in common or runner_owned in baseline or runner_owned in candidate:
            raise BenchmarkContractError(
                f"{runner_owned} is runner-owned; select the corresponding benchmark mode/profile"
            )
    for label, env, profile in (
        ("baseline", baseline, baseline_profile),
        ("candidate", candidate, candidate_profile),
    ):
        effective = {**common, **env}
        q4_global_variant = effective.get("TERMITE_METAL_Q4_0_MMV_VARIANT")
        if q4_global_variant is not None and q4_global_variant not in (
            "auto",
            "legacy",
            *Q4_MMV_VARIANTS,
        ):
            raise BenchmarkContractError(
                f"{label} global Q4 MMV variant must be auto, legacy, or one of "
                f"{', '.join(Q4_MMV_VARIANTS)}; got {q4_global_variant!r}"
            )
        for shape, name in GQA_SPLIT_VARIANT_ENV.items():
            value = effective.get(name)
            if value is not None and value not in ("auto", *GQA_SPLIT_VARIANTS):
                raise BenchmarkContractError(
                    f"{label} {shape} GQA split variant must be auto or one of "
                    f"{', '.join(GQA_SPLIT_VARIANTS)}; got {value!r}"
                )
            if profile != "gqa_split_schedule" and value is not None:
                raise BenchmarkContractError(
                    f"{label} sets {name} without route profile gqa_split_schedule"
                )
        for workload, name in Q4_MMV_WORKLOAD_ENV.items():
            value = effective.get(name)
            if value is not None and value not in ("auto", "legacy", *Q4_MMV_VARIANTS):
                raise BenchmarkContractError(
                    f"{label} {workload} Q4 MMV variant must be auto, legacy, or one of "
                    f"{', '.join(Q4_MMV_VARIANTS)}; got {value!r}"
                )
            if profile != "q4_mmv_workload" and value is not None:
                raise BenchmarkContractError(
                    f"{label} sets {name} without route profile q4_mmv_workload"
                )
            if (
                profile == "q4_mmv_workload"
                and workload not in Q4_MMV_WORKLOAD_DISPATCHES_PER_FRAME
                and value is not None
            ):
                raise BenchmarkContractError(
                    f"{label} sets {name}, but Gemma4 E4B has no observable row-one "
                    f"{workload} dispatches in the q4_mmv_workload contract"
                )
        if profile == "q4_mmv_workload":
            if effective.get("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO") not in (None, "0"):
                raise BenchmarkContractError(
                    f"{label} q4_mmv_workload profile cannot disable the Q4 MMV portfolio"
                )
            if effective.get("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE") not in (None, "0"):
                raise BenchmarkContractError(
                    f"{label} q4_mmv_workload profile cannot disable the small Q4 MMV kernels"
                )
        if profile == "concurrent_split":
            if env.get("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH") != "1":
                raise BenchmarkContractError(
                    f"{label} concurrent_split profile requires "
                    "TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH=1"
                )
            if env.get("TERMITE_METAL_DISABLE_CONCURRENT_PLANNED_DISPATCH") not in (None, "0"):
                raise BenchmarkContractError(
                    f"{label} concurrent_split profile cannot disable concurrent dispatch"
                )
        decode_pair_required = profile in ("pair_decode", "pair_decode_prefill")
        prefill_pair_required = profile in ("pair_prefill", "pair_decode_prefill")
        if (env.get("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION") == "1") != decode_pair_required:
            raise BenchmarkContractError(
                f"{label} route profile {profile} and decode pair-activation enable disagree"
            )
        if decode_pair_required and env.get(
            "TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION"
        ) not in (None, "0"):
            raise BenchmarkContractError(f"{label} decode pair activation is also disabled")
        if (env.get("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM") == "1") != prefill_pair_required:
            raise BenchmarkContractError(
                f"{label} route profile {profile} and prefill pair-activation enable disagree"
            )
        if prefill_pair_required and env.get(
            "TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_MM"
        ) not in (None, "0"):
            raise BenchmarkContractError(f"{label} prefill pair activation is also disabled")


def _effective_environment(
    inherited: dict[str, str],
    common: dict[str, str | None],
    variant: dict[str, str | None],
    stage_timing: bool,
    route_profile: str = "split_ffn",
) -> dict[str, str]:
    result = dict(inherited)
    for name in tuple(result):
        if name.startswith(POLICY_ENV_PREFIXES):
            result.pop(name, None)
    cleared = CONTROLLED_ENV_NAMES | common.keys() | variant.keys()
    for name in cleared:
        result.pop(name, None)
    for values in (common, variant):
        for name, value in values.items():
            if value is not None:
                result[name] = value
    result["TERMITE_METAL_STAGE_TIMING"] = "1" if stage_timing else "0"
    result[GQA_SPLIT_TRACE_ENV] = "1" if route_profile == "gqa_split_schedule" else "0"
    result[Q4_MMV_TRACE_ENV] = "1" if route_profile == "q4_mmv_workload" else "0"
    if stage_timing:
        result["TERMITE_METAL_STAGE_TIMING_PREFILL_MAX"] = str(
            STAGE_TIMING_SAMPLING["prefill_max"]
        )
        result["TERMITE_METAL_STAGE_TIMING_DECODE_START"] = str(
            STAGE_TIMING_SAMPLING["decode_start"]
        )
        result["TERMITE_METAL_STAGE_TIMING_DECODE_STRIDE"] = str(
            STAGE_TIMING_SAMPLING["decode_stride"]
        )
        result["TERMITE_METAL_STAGE_TIMING_DECODE_MAX"] = str(
            STAGE_TIMING_SAMPLING["decode_max"]
        )
    result["TERMITE_GEN_STAGE_DEBUG"] = "1"
    return result


def _effective_env_record(
    common: dict[str, str | None],
    variant: dict[str, str | None],
    variant_names: Iterable[str],
    route_profile: str = "split_ffn",
) -> dict[str, str | None]:
    names = sorted(CONTROLLED_ENV_NAMES | common.keys() | set(variant_names))
    merged = {**common, **variant}
    result = {name: merged.get(name) for name in names}
    result[GQA_SPLIT_TRACE_ENV] = "1" if route_profile == "gqa_split_schedule" else "0"
    result[Q4_MMV_TRACE_ENV] = "1" if route_profile == "q4_mmv_workload" else "0"
    return result


def _expected_gqa_split_variants(
    common: dict[str, str | None], variant: dict[str, str | None]
) -> dict[str, str]:
    effective = {**common, **variant}
    return {
        shape: (
            "s32"
            if effective.get(environment_name) in (None, "auto")
            else effective[environment_name]
        )
        for shape, environment_name in GQA_SPLIT_VARIANT_ENV.items()
    }


def _expected_q4_mmv_workload_variants(
    common: dict[str, str | None], variant: dict[str, str | None]
) -> dict[str, dict[str, str]]:
    effective = {**common, **variant}
    global_override = effective.get("TERMITE_METAL_Q4_0_MMV_VARIANT") or "auto"
    result: dict[str, dict[str, str]] = {}
    for workload in ("generic", "attention", "ffn_gate_up", "ffn_down"):
        workload_override = (
            effective.get(Q4_MMV_WORKLOAD_ENV[workload]) or "auto"
            if workload in Q4_MMV_WORKLOAD_ENV
            else "auto"
        )
        override = global_override if global_override != "auto" else workload_override
        if override in Q4_MMV_VARIANTS:
            selected = override
        elif override == "legacy":
            selected = (
                "nr4-nsg2"
                if workload in ("generic", "attention")
                else "nr8-nsg2"
            )
        else:
            # Current production AUTO policy: attention retains its legacy
            # small route and exact M4 Gemma4 FFN shapes use NR4/NSG2.
            selected = "nr4-nsg2"
        result[workload] = {
            "global": global_override,
            "override": override,
            "selected": selected,
        }
    return result


def _default_prompt(repeat: int) -> str:
    sentence = (
        "You answer questions about indexed files using only evidence. "
        "Evidence: Spella Caffe Logo.pdf is in /Users/timkaye/Downloads. "
        "Spella Caffe Logo Two Color.pdf is in /Users/timkaye/Downloads. "
        "Ignore unrelated source code. "
    )
    evidence = sentence * repeat
    return (
        f"<|turn>user\n{evidence}\n\nwhere are my Spella coffee assets?<turn|>\n"
        "<|turn>model\n<|channel>thought\n<channel|>"
    )


def _variant_order(index: int) -> tuple[str, str]:
    return ("baseline", "candidate") if index % 2 == 1 else ("candidate", "baseline")


def _invocation_plan(
    mode: str,
    warmups: int,
    runs: int,
    stage_timing_runs: int,
    output_tokens: int,
    warmup_output_tokens: int,
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    order = 0

    def append(kind: str, variant: str, index: int, tokens: int, stage_timing: bool) -> None:
        nonlocal order
        order += 1
        plan.append(
            {
                "order": order,
                "kind": kind,
                "variant": variant,
                "index": index,
                "label": f"{kind}-{variant}-{index:02d}",
                "output_tokens": tokens,
                "stage_timing": stage_timing,
            }
        )

    if mode == "paired":
        for index in range(1, warmups + 1):
            for variant in _variant_order(index):
                append("warmup", variant, index, warmup_output_tokens, False)
        for index in range(1, runs + 1):
            for variant in _variant_order(index):
                append("performance", variant, index, output_tokens, False)
        for index in range(1, stage_timing_runs + 1):
            for variant in _variant_order(index):
                append("stage_timing", variant, index, output_tokens, True)
    elif mode == "determinism":
        for index in range(1, warmups + 1):
            append("warmup", "candidate", index, warmup_output_tokens, False)
        for index in range(1, runs + 1):
            append("determinism", "candidate", index, output_tokens, False)
    else:
        for index in range(1, warmups + 1):
            append("warmup", "candidate", index, warmup_output_tokens, False)
        for index in range(1, stage_timing_runs + 1):
            append("stage_timing", "candidate", index, output_tokens, True)
    return plan


def _route_expectations(profile: str, output_tokens: int) -> dict[str, Any]:
    if profile not in ROUTE_PROFILES:
        raise BenchmarkContractError(f"unsupported route profile: {profile}")
    decode_frames = output_tokens - 1
    decode_pairs = 42 * decode_frames if profile in ("pair_decode", "pair_decode_prefill") else 0
    prefill_pairs = 42 if profile in ("pair_prefill", "pair_decode_prefill") else 0
    return {
        "decode_frames": decode_frames,
        "attention": 42 * decode_frames,
        "q4_row_one": 210 * decode_frames - 2 * decode_pairs,
        "q4_row_65_plus": 342 - 2 * prefill_pairs,
        "decode_pairs": decode_pairs,
        "prefill_pairs": prefill_pairs,
        "logical_decode_q4": 210 * decode_frames,
        "logical_prefill_q4": 342,
    }


def _parse_key_values(raw: str, label: str, path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, value in re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)=([0-9]+)\b", raw):
        if name in result:
            raise BenchmarkContractError(f"duplicate {label} key {name}: {path}")
        result[name] = int(value)
    if not result:
        raise BenchmarkContractError(f"empty {label}: {path}")
    return result


def _require_keys(values: dict[str, int], keys: Iterable[str], label: str, path: Path) -> None:
    missing = sorted(set(keys) - values.keys())
    if missing:
        raise BenchmarkContractError(f"missing {label} keys {', '.join(missing)}: {path}")


def _parse_pair_policy(log: str, path: Path, required: bool) -> dict[str, int] | None:
    matches = list(re.finditer(r"^metal_q4_0_pair_activation_policy:\s*(.+)$", log, re.MULTILINE))
    if not matches:
        if required:
            raise BenchmarkContractError(f"missing Q4_0 pair activation policy counters: {path}")
        return None
    match = matches[-1]
    values = _parse_key_values(match.group(1), "Q4_0 pair activation policy", path)
    _require_keys(values, PAIR_POLICY_KEYS, "Q4_0 pair activation policy", path)
    return {key: values[key] for key in PAIR_POLICY_KEYS}


def _parse_q4_mmv_workload_policy(
    log: str,
    path: Path,
    expected: dict[str, dict[str, str]] | None,
) -> dict[str, dict[str, Any]] | None:
    matches = list(
        re.finditer(
            r"^metal-q4-0-mmv\s+apple_family=(\d+)\s+workload=([a-z_]+)"
            r"\s+global=([a-z0-9-]+)\s+override=([a-z0-9-]+)"
            r"\s+shape=(\d+)x(\d+)\s+selected=([a-z0-9-]+)\s+fallback=(\d+)$",
            log,
            re.MULTILINE,
        )
    )
    if expected is None:
        if matches:
            raise BenchmarkContractError(f"unexpected Q4 MMV workload trace: {path}")
        return None
    observed: dict[str, dict[str, Any]] = {}
    for match in matches:
        workload = match.group(2)
        if workload in observed:
            raise BenchmarkContractError(
                f"duplicate Q4 MMV workload trace for {workload}: {path}"
            )
        observed[workload] = {
            "apple_family": int(match.group(1)),
            "global": match.group(3),
            "override": match.group(4),
            "in_dim": int(match.group(5)),
            "out_dim": int(match.group(6)),
            "selected": match.group(7),
            "fallback": int(match.group(8)),
        }
    if set(observed) != set(expected):
        raise BenchmarkContractError(
            f"Q4 MMV workload traces={sorted(observed)}, expected {sorted(expected)}: {path}"
        )
    for workload, contract in expected.items():
        actual = observed[workload]
        for key in ("global", "override", "selected"):
            if actual[key] != contract[key]:
                raise BenchmarkContractError(
                    f"Q4 MMV {workload} {key}={actual[key]}, expected {contract[key]}: {path}"
                )
        if actual["apple_family"] != 9 or actual["fallback"] != 0:
            raise BenchmarkContractError(
                f"Q4 MMV {workload} family/fallback="
                f"{actual['apple_family']}/{actual['fallback']}, expected 9/0: {path}"
            )
    return observed


def _parse_gqa_split_schedule(
    log: str,
    path: Path,
    *,
    required: bool,
    decode_frames: int,
    expected_variants: dict[str, str] | None,
) -> dict[str, Any] | None:
    matches = list(
        re.finditer(r"^metal_decode_gqa_split_schedule:\s*(.+)$", log, re.MULTILINE)
    )
    if not matches:
        if required:
            raise BenchmarkContractError(f"missing decode GQA split schedule counters: {path}")
        return None
    if not required:
        return None

    values = _parse_key_values(
        matches[-1].group(1), "decode GQA split schedule", path
    )
    missing = sorted(set(GQA_SPLIT_SCHEDULE_KEYS) - values.keys())
    extra = sorted(values.keys() - set(GQA_SPLIT_SCHEDULE_KEYS))
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if extra:
            details.append(f"unexpected {', '.join(extra)}")
        raise BenchmarkContractError(
            f"decode GQA split schedule schema mismatch ({'; '.join(details)}): {path}"
        )
    if expected_variants is None or set(expected_variants) != set(GQA_SPLIT_VARIANT_ENV):
        raise BenchmarkContractError(f"missing expected decode GQA split variants: {path}")
    for shape, variant in expected_variants.items():
        if variant not in GQA_SPLIT_VARIANTS:
            raise BenchmarkContractError(
                f"unsupported expected {shape} decode GQA split variant {variant!r}: {path}"
            )

    expected_shape_totals = {
        "swa": 35 * decode_frames,
        "global": 7 * decode_frames,
    }
    variant_total = sum(
        values[f"{shape}_{variant}"]
        for shape in ("swa", "global")
        for variant in GQA_SPLIT_VARIANTS
    )
    if values["legacy_total"] != values["swa_total"] + values["global_total"]:
        raise BenchmarkContractError(
            "decode GQA split legacy total does not equal SWA plus global totals: "
            f"{path}"
        )
    if values["legacy_total"] != variant_total:
        raise BenchmarkContractError(
            "decode GQA split legacy total does not equal per-shape/per-variant calls: "
            f"{path}"
        )
    expected_legacy_total = 42 * decode_frames
    if values["legacy_total"] != expected_legacy_total:
        raise BenchmarkContractError(
            f"decode GQA split legacy total={values['legacy_total']}, "
            f"expected {expected_legacy_total}: {path}"
        )
    for shape, expected_total in expected_shape_totals.items():
        observed_total = values[f"{shape}_total"]
        bucket_total = sum(
            values[f"{shape}_{variant}"] for variant in GQA_SPLIT_VARIANTS
        )
        if observed_total != expected_total or bucket_total != expected_total:
            raise BenchmarkContractError(
                f"decode GQA split {shape} total/buckets={observed_total}/{bucket_total}, "
                f"expected {expected_total}: {path}"
            )
        expected_variant = expected_variants[shape]
        expected_buckets = {
            variant: expected_total if variant == expected_variant else 0
            for variant in GQA_SPLIT_VARIANTS
        }
        observed_buckets = {
            variant: values[f"{shape}_{variant}"] for variant in GQA_SPLIT_VARIANTS
        }
        if observed_buckets != expected_buckets:
            raise BenchmarkContractError(
                f"decode GQA split {shape} variants={observed_buckets}, "
                f"expected {expected_buckets}: {path}"
            )
    if values["fallbacks"] != 0 or values["invalid_overrides"] != 0:
        raise BenchmarkContractError(
            "decode GQA split schedule reported fallback/invalid override="
            f"{values['fallbacks']}/{values['invalid_overrides']}: {path}"
        )
    return {
        **{key: values[key] for key in GQA_SPLIT_SCHEDULE_KEYS},
        "expected_variants": dict(sorted(expected_variants.items())),
    }


def _parse_stage_timing(
    log: str,
    path: Path,
    required: bool,
    decode_frames: int,
    metal: dict[str, Any],
) -> dict[str, int] | None:
    matches = list(re.finditer(r"^metal_stage_timing_ns:\s*(.+)$", log, re.MULTILINE))
    if not matches:
        raise BenchmarkContractError(f"missing Metal stage timing record: {path}")
    match = matches[-1]
    if not re.search(rf"\bscope={re.escape(STAGE_TIMING_SCOPE)}\b", match.group(1)):
        raise BenchmarkContractError(
            f"Metal stage timing scope is not {STAGE_TIMING_SCOPE}: {path}"
        )
    values = _parse_key_values(match.group(1), "Metal stage timing", path)
    _require_keys(values, STAGE_TIMING_KEYS, "Metal stage timing", path)
    result = {key: values[key] for key in STAGE_TIMING_KEYS}
    stage_json = _mapping(metal.get("stage_timing_ns"), "Metal stage timing JSON", path)
    if stage_json.get("scope") != STAGE_TIMING_SCOPE:
        raise BenchmarkContractError(
            f"metal.stage_timing_ns.scope is not {STAGE_TIMING_SCOPE}: {path}"
        )
    for key in ("enabled", "supported", "complete", "samples", "failures"):
        _exact_int(stage_json, key, result[key], "metal.stage_timing_ns", path)
    for phase in ("prefill", "decode"):
        phase_json = _mapping(
            stage_json.get(phase), f"Metal {phase} stage timing JSON", path
        )
        for json_key, flat_key in (
            ("frames", f"{phase}_frames"),
            ("gpu", f"{phase}_gpu"),
            *((name, f"{phase}_{name}") for name in STAGE_NAMES),
        ):
            _exact_int(
                phase_json,
                json_key,
                result[flat_key],
                f"metal.stage_timing_ns.{phase}",
                path,
            )
    if not required:
        if any(result.values()):
            raise BenchmarkContractError(
                f"performance run unexpectedly enabled or sampled stage timing: {path}"
            )
        return None
    for key in ("enabled", "supported", "complete"):
        if result[key] != 1:
            raise BenchmarkContractError(f"Metal stage timing {key}={result[key]}, expected 1: {path}")
    if result["failures"] != 0:
        raise BenchmarkContractError(f"Metal stage timing failures={result['failures']}: {path}")
    expected_decode_frames = 0
    start = STAGE_TIMING_SAMPLING["decode_start"]
    if decode_frames > start:
        expected_decode_frames = min(
            STAGE_TIMING_SAMPLING["decode_max"],
            ((decode_frames - 1 - start) // STAGE_TIMING_SAMPLING["decode_stride"]) + 1,
        )
    if result["prefill_frames"] != 1 or result["decode_frames"] != expected_decode_frames:
        raise BenchmarkContractError(
            "Metal stage timing frame selection mismatch: "
            f"prefill/decode={result['prefill_frames']}/{result['decode_frames']}, "
            f"expected 1/{expected_decode_frames}: {path}"
        )
    if result["samples"] < 2 * (result["prefill_frames"] + result["decode_frames"]):
        raise BenchmarkContractError(
            f"Metal stage timing samples={result['samples']} are insufficient for "
            f"frames={result['prefill_frames'] + result['decode_frames']}: {path}"
        )
    for phase in ("prefill", "decode"):
        gpu = result[f"{phase}_gpu"]
        if gpu <= 0:
            raise BenchmarkContractError(f"Metal {phase} stage GPU time is non-positive: {path}")
        attributed = sum(result[f"{phase}_{name}"] for name in STAGE_NAMES)
        if attributed != gpu:
            raise BenchmarkContractError(
                f"Metal {phase} stages do not reconcile: attributed={attributed}ns "
                f"gpu={gpu}ns: {path}"
            )
    return result


def parse_antfly_sample(
    json_path: Path,
    log_path: Path,
    *,
    output_tokens: int,
    expected_prompt_tokens: int,
    expected_token_sha256: str | None,
    expected_prompt_sha256: str,
    route_profile: str,
    expected_q4_mmv_variant: str,
    expected_q4_mmv_workloads: dict[str, dict[str, str]] | None,
    expected_pair_mmv_variant: str,
    expected_pair_mm_variant: str,
    expected_metal_device: str,
    stage_timing: bool,
    expected_gqa_split_variants: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"invalid Antfly JSON: {json_path}: {exc}") from exc
    log = log_path.read_text(errors="replace")
    if payload.get("backend") != "metal":
        raise BenchmarkContractError(f"Antfly did not report Metal: {json_path}")
    if payload.get("tokens") != output_tokens or payload.get("finish_reason") != "length":
        raise BenchmarkContractError(
            f"Antfly did not generate exactly {output_tokens} length-limited tokens: {json_path}"
        )
    if "speculative" not in payload or payload.get("speculative") is not None:
        raise BenchmarkContractError(f"baseline A/B experiment unexpectedly used speculation: {json_path}")
    for key in ("draft_cuda", "draft_cuda_generate"):
        if payload.get(key) is not None:
            raise BenchmarkContractError(f"baseline A/B experiment reported {key}: {json_path}")
    if "generate-setup: live whole-model executor skipped" not in log:
        raise BenchmarkContractError(f"compiled generation marker missing: {log_path}")
    if "gen_debug: executePrefill whole-model fast path" not in log:
        raise BenchmarkContractError(f"compiled prefill marker missing: {log_path}")

    token_text, token_ids = _token_line(log, "token_ids", log_path)
    prompt_text, prompt_ids = _token_line(log, "prompt_token_ids", log_path)
    token_sha = _sha256_text(token_text)
    prompt_sha = _sha256_text(prompt_text)
    if len(token_ids) != output_tokens:
        raise BenchmarkContractError(
            f"output token count={len(token_ids)}, expected {output_tokens}: {log_path}"
        )
    if len(prompt_ids) != expected_prompt_tokens:
        raise BenchmarkContractError(
            f"prompt token count={len(prompt_ids)}, expected {expected_prompt_tokens}: {log_path}"
        )
    if expected_token_sha256 is not None and token_sha != expected_token_sha256:
        raise BenchmarkContractError(
            f"output token digest={token_sha}, expected {expected_token_sha256}: {log_path}"
        )
    if prompt_sha != expected_prompt_sha256:
        raise BenchmarkContractError(
            f"prompt token digest={prompt_sha}, expected {expected_prompt_sha256}: {log_path}"
        )
    json_ids = payload.get("token_ids")
    if json_ids is not None and json_ids != token_ids:
        raise BenchmarkContractError(f"Antfly JSON/log token IDs differ: {json_path}")

    timing = _mapping(payload.get("timing_ms"), "Antfly timing", json_path)
    total_ms = _positive_finite(float(timing.get("generate") or 0), "Antfly total", json_path)
    prefill_ms = _positive_finite(
        float(timing.get("prefill_inner") or 0), "Antfly prefill", json_path
    )
    decode_ms = _positive_finite(
        float(timing.get("decode_inner") or 0), "Antfly decode", json_path
    )
    if abs(prefill_ms + decode_ms - total_ms) > max(2.0, total_ms * 0.001):
        raise BenchmarkContractError(
            f"Antfly phase timing does not reconcile: prefill+decode={prefill_ms + decode_ms:.3f}ms "
            f"total={total_ms:.3f}ms: {json_path}"
        )

    expected = _route_expectations(route_profile, output_tokens)
    attention_match = _last_match(
        log,
        (
            r"^metal_attention_dispatch:.*\bpaged_1x=(\d+)"
            r".*\bdecode_gqa_split=(\d+)"
            r".*\bgenerated_flash_prefill=(\d+)"
            r".*\bgenerated_flash_prefill_hd512=(\d+)"
            r".*\bprefill_direct_kv=(\d+)"
            r".*\bprefill_paged_kv=(\d+)"
        ),
        "attention route counters",
        log_path,
    )
    attention_values = tuple(int(attention_match.group(index)) for index in range(1, 7))
    expected_attention = (0, expected["attention"], 35, 7, 0, 42)
    if attention_values != expected_attention:
        raise BenchmarkContractError(
            f"attention routes={attention_values}, expected {expected_attention}: {log_path}"
        )
    gqa_split_schedule = _parse_gqa_split_schedule(
        log,
        log_path,
        required=route_profile == "gqa_split_schedule",
        decode_frames=expected["decode_frames"],
        expected_variants=expected_gqa_split_variants,
    )

    prepared_match = _last_match(
        log,
        r"^metal_prepared_frame:\s+fast_path=(\d+)\s+fallback=(\d+)",
        "prepared frame route counters",
        log_path,
    )
    prepared = (int(prepared_match.group(1)), int(prepared_match.group(2)))
    if prepared != (expected["decode_frames"], 0):
        raise BenchmarkContractError(
            f"prepared frame routes={prepared}, expected {(expected['decode_frames'], 0)}: {log_path}"
        )

    memory_match = _last_match(
        log,
        r"^metal_runtime_memory:.*\bframe_retained_mb=(\d+)",
        "Metal runtime memory counters",
        log_path,
    )
    if int(memory_match.group(1)) != 0:
        raise BenchmarkContractError(f"compiled decoder retained a speculative frame: {log_path}")

    q4_match = _last_match(
        log,
        r"^metal_q4_0_dispatch:.*\blinear_reduce_rows=(\d+)/(\d+)/(\d+)/(\d+).*\bpair_act_reduce=(\d+)",
        "Q4_0 route counters",
        log_path,
    )
    q4_rows = tuple(int(q4_match.group(index)) for index in range(1, 5))
    pair_activation_dispatches = int(q4_match.group(5))
    expected_q4_rows = (expected["q4_row_one"], 0, 0, expected["q4_row_65_plus"])
    expected_pair_activation_dispatches = expected["decode_pairs"] + expected["prefill_pairs"]
    if q4_rows != expected_q4_rows or pair_activation_dispatches != expected_pair_activation_dispatches:
        raise BenchmarkContractError(
            f"Q4 routes rows={q4_rows}, pair_activation_dispatches={pair_activation_dispatches}; expected "
            f"rows={expected_q4_rows}, pair_activation_dispatches={expected_pair_activation_dispatches}: {log_path}"
        )
    if q4_rows[0] + 2 * expected["decode_pairs"] != expected["logical_decode_q4"]:
        raise BenchmarkContractError(f"Q4 decode logical route invariant failed: {log_path}")

    q4_policy_match = _last_match(
        log,
        (
            r"^metal_q4_0_policy:\s+mmv_nr4_nsg2=(\d+)\s+mmv_nr8_nsg2=(\d+)"
            r"\s+mmv_nr4_nsg4=(\d+)\s+mmv_nr8_nsg4=(\d+)"
            r"\s+mmv_variant_fallbacks=(\d+)"
            r"\s+mm_sg_aligned=(\d+)\s+mm_sg_aligned_tail=(\d+)\s+mm_sg_unrolled=(\d+)"
        ),
        "Q4_0 policy counters",
        log_path,
    )
    q4_policy_values = tuple(int(q4_policy_match.group(index)) for index in range(1, 9))
    q4_variants = q4_policy_values[:4]
    if q4_policy_values[4] != 0:
        raise BenchmarkContractError(f"Q4 MMV variant fallback={q4_policy_values[4]}: {log_path}")
    variant_names = Q4_MMV_VARIANTS
    if expected_q4_mmv_variant not in variant_names:
        raise BenchmarkContractError(f"unsupported expected Q4 MMV variant: {expected_q4_mmv_variant}")
    expected_q4_variants = [0, 0, 0, 0]
    if expected_q4_mmv_workloads is None:
        expected_q4_variants[variant_names.index(expected_q4_mmv_variant)] = q4_rows[0]
    else:
        workload_dispatches = {
            workload: dispatches * expected["decode_frames"]
            for workload, dispatches in Q4_MMV_WORKLOAD_DISPATCHES_PER_FRAME.items()
        }
        if set(expected_q4_mmv_workloads) != set(workload_dispatches):
            raise BenchmarkContractError(f"invalid Q4 MMV workload contract: {log_path}")
        for workload, dispatches in workload_dispatches.items():
            selected = expected_q4_mmv_workloads[workload]["selected"]
            if selected not in variant_names:
                raise BenchmarkContractError(
                    f"unsupported expected {workload} Q4 MMV variant {selected}: {log_path}"
                )
            expected_q4_variants[variant_names.index(selected)] += dispatches
    if list(q4_variants) != expected_q4_variants:
        raise BenchmarkContractError(
            f"Q4 MMV variants={q4_variants}, expected {tuple(expected_q4_variants)}: {log_path}"
        )
    q4_mmv_workload_policy = _parse_q4_mmv_workload_policy(
        log,
        log_path,
        expected_q4_mmv_workloads,
    )

    pair_required = expected["decode_pairs"] > 0 or expected["prefill_pairs"] > 0
    pair_policy = _parse_pair_policy(log, log_path, pair_required)
    if pair_policy is not None:
        if pair_policy["mmv_variant_fallbacks"] != 0 or pair_policy["mm_variant_fallbacks"] != 0:
            raise BenchmarkContractError(f"Q4 pair activation route fallback: {log_path}")
        pair_mmv_names = variant_names
        if expected_pair_mmv_variant not in pair_mmv_names:
            raise BenchmarkContractError(
                f"unsupported expected pair MMV variant: {expected_pair_mmv_variant}"
            )
        observed_pair_mmv = tuple(pair_policy[f"mmv_{name.replace('-', '_')}"] for name in pair_mmv_names)
        expected_pair_mmv = [0, 0, 0, 0]
        expected_pair_mmv[pair_mmv_names.index(expected_pair_mmv_variant)] = expected["decode_pairs"]
        if list(observed_pair_mmv) != expected_pair_mmv:
            raise BenchmarkContractError(
                f"pair MMV variants={observed_pair_mmv}, expected {tuple(expected_pair_mmv)}: {log_path}"
            )
        pair_mm_names = ("m32-n64-aligned", "m32-n64-tail", "m32-n32-aligned", "m32-n32-tail")
        if expected_pair_mm_variant not in pair_mm_names:
            raise BenchmarkContractError(
                f"unsupported expected pair MM variant: {expected_pair_mm_variant}"
            )
        observed_pair_mm = tuple(pair_policy[f"mm_{name.replace('-', '_')}"] for name in pair_mm_names)
        expected_pair_mm = [0, 0, 0, 0]
        expected_pair_mm[pair_mm_names.index(expected_pair_mm_variant)] = expected["prefill_pairs"]
        if list(observed_pair_mm) != expected_pair_mm:
            raise BenchmarkContractError(
                f"pair MM routes={observed_pair_mm}, expected {tuple(expected_pair_mm)}: {log_path}"
            )
    if q4_rows[3] + 2 * expected["prefill_pairs"] != expected["logical_prefill_q4"]:
        raise BenchmarkContractError(f"Q4 prefill logical route invariant failed: {log_path}")

    q6_match = _last_match(
        log,
        r"^metal_q4_q6_k_dispatch:.*\bq6_linear_reduce_rows=(\d+)/(\d+)/(\d+)/(\d+)",
        "Q6_K route counters",
        log_path,
    )
    q6_rows = tuple(int(q6_match.group(index)) for index in range(1, 5))
    if q6_rows != (output_tokens, 0, 0, 0):
        raise BenchmarkContractError(
            f"Q6_K routes={q6_rows}, expected {(output_tokens, 0, 0, 0)}: {log_path}"
        )

    runtime = _mapping(payload.get("runtime"), "runtime counters", json_path)
    decoder = _mapping(payload.get("generation_decoder_runtime"), "decoder counters", json_path)
    _exact_int(runtime, "decode_greedy_calls", expected["decode_frames"], "runtime", json_path)
    _exact_int(
        decoder,
        "forward_attempts",
        expected["decode_frames"],
        "generation_decoder_runtime",
        json_path,
    )
    metal = _mapping(payload.get("metal"), "Metal counters", json_path)
    metal_device = metal.get("device")
    if metal_device != expected_metal_device:
        raise BenchmarkContractError(
            f"Metal device={metal_device!r}, expected {expected_metal_device!r}: {json_path}"
        )
    metal_device_registry_id = metal.get("device_registry_id")
    if (
        isinstance(metal_device_registry_id, bool)
        or not isinstance(metal_device_registry_id, int)
        or metal_device_registry_id <= 0
    ):
        raise BenchmarkContractError(
            "Metal device_registry_id must be a positive integer, got "
            f"{metal_device_registry_id!r}: {json_path}"
        )
    if metal.get("native_quant_null") is not False:
        raise BenchmarkContractError(f"Metal native quant route unavailable: {json_path}")
    operators = _mapping(metal.get("runtime_command_operators"), "operator counters", json_path)
    _exact_int(operators, "fallback", 0, "metal.runtime_command_operators", json_path)
    fallbacks = _mapping(metal.get("frame_fallbacks"), "frame fallback counters", json_path)
    for key in ("decode_fallback", "prefill_plan_fail", "prefill_execute_fail"):
        _exact_int(fallbacks, key, 0, "metal.frame_fallbacks", json_path)
    quant_plan = _mapping(metal.get("quant_kernel_plan"), "quant plan counters", json_path)
    for key in ("fast_path_misses", "unsupported_routes"):
        _exact_int(quant_plan, key, 0, "metal.quant_kernel_plan", json_path)
    attention_json = _mapping(metal.get("attention_dispatch"), "attention counters", json_path)
    for key, value in zip(
        (
            "paged_1x",
            "decode_gqa_split",
            "generated_flash_prefill",
            "generated_flash_prefill_hd512",
            "prefill_direct_kv",
            "prefill_paged_kv",
        ),
        attention_values,
        strict=True,
    ):
        _exact_int(attention_json, key, value, "metal.attention_dispatch", json_path)
    prepared_json = _mapping(metal.get("prepared_frame"), "prepared frame counters", json_path)
    _exact_int(prepared_json, "fast_path", prepared[0], "metal.prepared_frame", json_path)
    _exact_int(prepared_json, "fallback", prepared[1], "metal.prepared_frame", json_path)

    profile = _parse_stage_timing(
        log,
        log_path,
        stage_timing,
        expected["decode_frames"],
        metal,
    )
    decode_tps = expected["decode_frames"] * 1000.0 / decode_ms
    return {
        "output_tokens": output_tokens,
        "prompt_tokens": len(prompt_ids),
        "token_ids_sha256": token_sha,
        "prompt_token_ids_sha256": prompt_sha,
        "total_ms": total_ms,
        "prefill_ms": prefill_ms,
        "decode_ms": decode_ms,
        "decode_tok_s": decode_tps,
        "metal_device": metal_device,
        "metal_device_registry_id": metal_device_registry_id,
        "route_profile": route_profile,
        "routes": {
            "paged_1x": attention_values[0],
            "decode_gqa_split": attention_values[1],
            **(
                {"q4_mmv_workload_policy": q4_mmv_workload_policy}
                if q4_mmv_workload_policy is not None
                else {}
            ),
            **(
                {"gqa_split_schedule": gqa_split_schedule}
                if gqa_split_schedule is not None
                else {}
            ),
            "generated_flash_prefill": attention_values[2],
            "generated_flash_prefill_hd512": attention_values[3],
            "prefill_direct_kv": attention_values[4],
            "prefill_paged_kv": attention_values[5],
            "prepared_frame_fast": prepared[0],
            "prepared_frame_fallback": prepared[1],
            "q4_linear_reduce_rows": list(q4_rows),
            "q4_pair_activation_decode": expected["decode_pairs"],
            "q4_pair_activation_total": pair_activation_dispatches,
            "q4_mmv_variants": list(q4_variants),
            "q4_mmv_variant_fallbacks": q4_policy_values[4],
            "q4_pair_activation_policy": pair_policy,
            "q6_linear_reduce_rows": list(q6_rows),
            "frame_retained_mb": 0,
        },
        "stage_timing_ns": profile,
        "exact_token_contract_passed": True,
        "route_contract_passed": True,
    }


def _verify_sha(value: Any, label: str, path: Path) -> str:
    if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
        raise BenchmarkContractError(f"missing immutable SHA-256 {label}: {path}")
    return value


def _sysctl(name: str) -> str:
    try:
        return subprocess.run(
            ["sysctl", "-n", name], capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except Exception:
        return "unknown"


_NOMINAL_GB_S = {
    "Apple M4 Max": 546.0,
    "Apple M4 Pro": 273.0,
    "Apple M4": 120.0,
    "Apple M3 Max": 400.0,
    "Apple M3 Pro": 150.0,
    "Apple M3": 100.0,
}


def _nominal_bandwidth_gb_s(chip: str) -> float | None:
    for prefix, gb_s in _NOMINAL_GB_S.items():
        if chip.startswith(prefix):
            return gb_s
    return None


def _thermal_speed_limit_pct() -> int | None:
    try:
        out = subprocess.run(
            ["pmset", "-g", "therm"], capture_output=True, text=True, timeout=5
        ).stdout
    except Exception:
        return None
    for line in out.splitlines():
        if "CPU_Speed_Limit" in line:
            try:
                return int(line.split("=")[-1].strip())
            except ValueError:
                return None
    return None


def _load_metadata(root: Path) -> dict[str, Any]:
    path = root / "metadata.json"
    try:
        metadata = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"invalid A/B metadata: {path}: {exc}") from exc
    if not isinstance(metadata, dict) or metadata.get("schema") != METADATA_SCHEMA:
        raise BenchmarkContractError(f"unsupported A/B metadata schema: {path}")
    for key in (
        "runner_sha256",
        "shared_parser_sha256",
        "gguf_sha256",
        "antfly_binary_sha256",
        "prompt_sha256",
        "expected_token_ids_sha256",
        "expected_prompt_token_ids_sha256",
        "git_status_sha256",
        "git_tracked_diff_sha256",
    ):
        _verify_sha(metadata.get(key), key, path)
    if not isinstance(metadata.get("expected_metal_device"), str) or not metadata[
        "expected_metal_device"
    ].strip():
        raise BenchmarkContractError(f"missing expected Metal device provenance: {path}")
    current_sources = {
        "runner_sha256": _file_sha256(Path(__file__).resolve()),
        "shared_parser_sha256": _file_sha256(SHARED_PARSER),
        "gguf_sha256": _file_sha256(Path(metadata["gguf"])),
        "antfly_binary_sha256": _file_sha256(Path(metadata["antfly_bin"])),
    }
    for key, current in current_sources.items():
        if metadata[key] != current:
            raise BenchmarkContractError(
                f"A/B provenance mismatch for {key}: recorded={metadata[key]}, current={current}: {path}"
            )
    prompt_path = root / "prompt.txt"
    if not prompt_path.is_file() or _file_sha256(prompt_path) != metadata["prompt_sha256"]:
        raise BenchmarkContractError(f"A/B prompt provenance mismatch: {prompt_path}")
    repo = Path(metadata["repo_root"])
    current_git = _git_provenance(repo)
    for key in ("git_revision", "git_dirty", "git_status_sha256", "git_tracked_diff_sha256"):
        if metadata.get(key) != current_git[key]:
            raise BenchmarkContractError(f"A/B git provenance changed for {key}: {path}")
    if metadata.get("mode") not in ("paired", "determinism", "stage"):
        raise BenchmarkContractError(f"invalid A/B mode: {path}")
    if metadata.get("baseline_route_profile") not in ROUTE_PROFILES:
        raise BenchmarkContractError(f"invalid baseline route profile: {path}")
    if metadata.get("candidate_route_profile") not in ROUTE_PROFILES:
        raise BenchmarkContractError(f"invalid candidate route profile: {path}")
    expected_isolation_contract = {
        "clear_inherited_prefixes": list(POLICY_ENV_PREFIXES),
        "reapply_only_explicit_maps": True,
        "runner_owned": sorted(RUNNER_OWNED_ENV_NAMES),
        "runner_owned_values": {
            "TERMITE_GEN_STAGE_DEBUG": "1",
            "TERMITE_METAL_STAGE_TIMING": "1 when invocation.stage_timing is true, otherwise 0",
            GQA_SPLIT_TRACE_ENV: "1 only for gqa_split_schedule route profile, otherwise 0",
            Q4_MMV_TRACE_ENV: "1 only for q4_mmv_workload route profile, otherwise 0",
        },
    }
    if metadata.get("environment_isolation_contract") != expected_isolation_contract:
        raise BenchmarkContractError(f"A/B environment isolation contract was modified: {path}")
    environments: dict[str, dict[str, str | None]] = {}
    for label in ("common", "baseline", "candidate"):
        raw = metadata.get(f"{label}_env")
        if not isinstance(raw, dict):
            raise BenchmarkContractError(f"missing {label} environment provenance: {path}")
        rendered: list[str] = []
        for name, value in raw.items():
            if not isinstance(name, str) or (value is not None and not isinstance(value, str)):
                raise BenchmarkContractError(f"invalid {label} environment provenance: {path}")
            rendered.append(name if value is None else f"{name}={value}")
        environments[label] = _parse_env_entries(rendered, label)
    _validate_variant_environments(
        environments["common"],
        environments["baseline"],
        environments["candidate"],
        metadata["baseline_route_profile"],
        metadata["candidate_route_profile"],
    )
    variant_names = environments["baseline"].keys() | environments["candidate"].keys()
    for variant in ("baseline", "candidate"):
        expected_effective = _effective_env_record(
            environments["common"],
            environments[variant],
            variant_names,
            metadata[f"{variant}_route_profile"],
        )
        if metadata.get(f"effective_{variant}_env") != expected_effective:
            raise BenchmarkContractError(
                f"effective {variant} environment provenance was modified: {path}"
            )
        expected_gqa_variants = _expected_gqa_split_variants(
            environments["common"], environments[variant]
        )
        if metadata.get(f"{variant}_gqa_split_variants") != expected_gqa_variants:
            raise BenchmarkContractError(
                f"expected {variant} GQA split variant provenance was modified: {path}"
            )
        expected_q4_workloads = _expected_q4_mmv_workload_variants(
            environments["common"], environments[variant]
        )
        if metadata.get(f"{variant}_q4_mmv_workloads") != expected_q4_workloads:
            raise BenchmarkContractError(
                f"expected {variant} Q4 MMV workload provenance was modified: {path}"
            )
    gqa_contract = metadata.get("gqa_split_schedule_contract")
    expected_gqa_contract = {
        "log_prefix": "metal_decode_gqa_split_schedule:",
        "trace_environment": f"{GQA_SPLIT_TRACE_ENV}=1",
        "variant_environments": GQA_SPLIT_VARIANT_ENV,
        "variants": list(GQA_SPLIT_VARIANTS),
        "default_variant": "s32",
        "final_snapshot_wins": True,
    }
    if gqa_contract != expected_gqa_contract:
        raise BenchmarkContractError(f"GQA split schedule contract was modified: {path}")
    expected_q4_contract = {
        "log_prefix": "metal-q4-0-mmv",
        "trace_environment": f"{Q4_MMV_TRACE_ENV}=1",
        "variant_environments": Q4_MMV_WORKLOAD_ENV,
        "variants": list(Q4_MMV_VARIANTS),
        "dispatches_per_decode_frame": Q4_MMV_WORKLOAD_DISPATCHES_PER_FRAME,
        "global_override_precedence": True,
    }
    if metadata.get("q4_mmv_workload_contract") != expected_q4_contract:
        raise BenchmarkContractError(f"Q4 MMV workload contract was modified: {path}")
    stage_contract = metadata.get("stage_timing_contract")
    if not isinstance(stage_contract, dict) or stage_contract.get("sampling") != STAGE_TIMING_SAMPLING:
        raise BenchmarkContractError(f"stage timing sampling contract was modified: {path}")
    if stage_contract.get("scope") != STAGE_TIMING_SCOPE:
        raise BenchmarkContractError(f"stage timing scope contract was modified: {path}")
    plan = _invocation_plan(
        metadata["mode"],
        metadata["warmups"],
        metadata["runs"],
        metadata["stage_timing_runs"],
        metadata["output_tokens"],
        metadata["warmup_output_tokens"],
    )
    if metadata.get("invocations") != plan:
        raise BenchmarkContractError(f"A/B invocation order was modified: {path}")
    return metadata


def _validate_artifact_set(root: Path, invocations: list[dict[str, Any]]) -> None:
    expected_json = {"metadata.json", *(f"{item['label']}.json" for item in invocations)}
    allowed_json = expected_json | {"summary.json"}
    expected_logs = {f"{item['label']}.log" for item in invocations}
    observed_json = {path.name for path in root.glob("*.json")}
    observed_logs = {path.name for path in root.glob("*.log")}
    if not expected_json <= observed_json:
        missing = sorted(expected_json - observed_json)
        raise BenchmarkContractError(f"missing A/B JSON artifacts: {', '.join(missing)}")
    if observed_json - allowed_json:
        raise BenchmarkContractError(
            f"unexpected A/B JSON artifacts: {', '.join(sorted(observed_json - allowed_json))}"
        )
    if observed_logs != expected_logs:
        missing = sorted(expected_logs - observed_logs)
        extra = sorted(observed_logs - expected_logs)
        raise BenchmarkContractError(
            f"A/B log artifact mismatch: missing={missing}, unexpected={extra}"
        )


def _metric_stats(samples: list[dict[str, Any]], variant: str, field: str) -> dict[str, float]:
    return stats(sample[field] for sample in samples if sample["variant"] == variant)


def _ratio_stats(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    return stats(row[field] for row in rows)


def build_summary(root: Path) -> dict[str, Any]:
    metadata = _load_metadata(root)
    invocations = metadata["invocations"]
    _validate_artifact_set(root, invocations)
    parsed: list[dict[str, Any]] = []
    warmup_reference: str | None = metadata.get("expected_warmup_token_ids_sha256")
    for invocation in invocations:
        variant = invocation["variant"]
        route_profile = metadata[f"{variant}_route_profile"]
        expected_sha = metadata["expected_token_ids_sha256"]
        if invocation["kind"] == "warmup":
            expected_sha = warmup_reference
        sample = parse_antfly_sample(
            root / f"{invocation['label']}.json",
            root / f"{invocation['label']}.log",
            output_tokens=invocation["output_tokens"],
            expected_prompt_tokens=metadata["expected_prompt_tokens"],
            expected_token_sha256=expected_sha,
            expected_prompt_sha256=metadata["expected_prompt_token_ids_sha256"],
            route_profile=route_profile,
            expected_q4_mmv_variant=metadata["expected_q4_mmv_variant"],
            expected_q4_mmv_workloads=(
                metadata[f"{variant}_q4_mmv_workloads"]
                if route_profile == "q4_mmv_workload"
                else None
            ),
            expected_pair_mmv_variant=metadata["expected_pair_mmv_variant"],
            expected_pair_mm_variant=metadata["expected_pair_mm_variant"],
            expected_metal_device=metadata["expected_metal_device"],
            stage_timing=invocation["stage_timing"],
            expected_gqa_split_variants=metadata[f"{variant}_gqa_split_variants"],
        )
        if invocation["kind"] == "warmup":
            if warmup_reference is None:
                warmup_reference = sample["token_ids_sha256"]
            elif sample["token_ids_sha256"] != warmup_reference:
                raise BenchmarkContractError(
                    f"warmup token IDs changed between variants/runs: {invocation['label']}"
                )
        parsed.append({**invocation, **sample})

    registry_ids = {sample["metal_device_registry_id"] for sample in parsed}
    if len(registry_ids) != 1:
        raise BenchmarkContractError(
            f"Metal device registry ID changed between A/B samples: {sorted(registry_ids)}"
        )
    metal_device_registry_id = next(iter(registry_ids))

    performance = [sample for sample in parsed if sample["kind"] == "performance"]
    stage_samples = [sample for sample in parsed if sample["kind"] == "stage_timing"]
    warmups = [sample for sample in parsed if sample["kind"] == "warmup"]
    determinism = [sample for sample in parsed if sample["kind"] == "determinism"]
    checks: dict[str, Any] = {
        "exact_tokens": True,
        "routes": True,
        "stage_timing_excluded_from_performance": not any(
            sample["stage_timing"] for sample in performance
        ),
    }
    failures: list[str] = []
    result: dict[str, Any] = {
        "schema": SUMMARY_SCHEMA,
        "metadata": metadata,
        "mode": metadata["mode"],
        "experiment_id": metadata["experiment_id"],
        "output_tokens": metadata["output_tokens"],
        "prompt_tokens": metadata["expected_prompt_tokens"],
        "expected_token_ids_sha256": metadata["expected_token_ids_sha256"],
        "expected_prompt_token_ids_sha256": metadata["expected_prompt_token_ids_sha256"],
        "metal_device_registry_id": metal_device_registry_id,
        "warmup_token_ids_sha256": warmup_reference,
        "performance_samples": performance,
        "stage_timing_samples": stage_samples,
        "warmup_samples": warmups,
        "checks": checks,
    }
    if metadata["mode"] == "determinism":
        digests = {sample["token_ids_sha256"] for sample in determinism}
        prompt_digests = {sample["prompt_token_ids_sha256"] for sample in determinism}
        checks["determinism_runs"] = len(determinism) == metadata["runs"]
        checks["deterministic_output"] = digests == {metadata["expected_token_ids_sha256"]}
        checks["deterministic_prompt"] = prompt_digests == {
            metadata["expected_prompt_token_ids_sha256"]
        }
        if not all(checks.values()):
            failures.append("short determinism contract failed")
        result.update(
            {
                "determinism_samples": determinism,
                "unique_output_token_digests": sorted(digests),
                "unique_prompt_token_digests": sorted(prompt_digests),
                "failures": failures,
                "passed": not failures,
            }
        )
        return result
    if metadata["mode"] == "stage":
        checks["stage_timing_runs"] = len(stage_samples) == metadata["stage_timing_runs"]
        checks["no_performance_samples"] = not performance
        if not all(checks.values()):
            failures.append("stage-only profiling contract failed")
        result.update({"failures": failures, "passed": not failures})
        return result

    pairs: list[dict[str, Any]] = []
    for index in range(1, metadata["runs"] + 1):
        indexed = [sample for sample in performance if sample["index"] == index]
        by_variant = {sample["variant"]: sample for sample in indexed}
        if set(by_variant) != {"baseline", "candidate"} or len(indexed) != 2:
            raise BenchmarkContractError(f"invalid performance pair {index}")
        baseline = by_variant["baseline"]
        candidate = by_variant["candidate"]
        pairs.append(
            {
                "pair": index,
                "execution_order": list(_variant_order(index)),
                "candidate_total_latency_ratio": candidate["total_ms"] / baseline["total_ms"],
                "candidate_prefill_latency_ratio": candidate["prefill_ms"] / baseline["prefill_ms"],
                "candidate_decode_latency_ratio": candidate["decode_ms"] / baseline["decode_ms"],
                "candidate_decode_throughput_ratio": candidate["decode_tok_s"]
                / baseline["decode_tok_s"],
            }
        )
    metrics = {
        f"{variant}_{phase}": _metric_stats(performance, variant, field)
        for variant in ("baseline", "candidate")
        for phase, field in (
            ("total_ms", "total_ms"),
            ("prefill_ms", "prefill_ms"),
            ("decode_ms", "decode_ms"),
            ("decode_tok_s", "decode_tok_s"),
        )
    }
    paired_ratios = {
        name: _ratio_stats(pairs, name)
        for name in (
            "candidate_total_latency_ratio",
            "candidate_prefill_latency_ratio",
            "candidate_decode_latency_ratio",
            "candidate_decode_throughput_ratio",
        )
    }
    cv_violations = {
        name: value["cv"]
        for name, value in metrics.items()
        if not name.endswith("decode_tok_s") and value["cv"] > metadata["thresholds"]["max_cv"]
    }
    target_field = {
        "total": "candidate_total_latency_ratio",
        "prefill": "candidate_prefill_latency_ratio",
        "decode": "candidate_decode_latency_ratio",
    }[metadata["target_phase"]]
    target_wins = sum(pair[target_field] < 1.0 for pair in pairs)
    thresholds = metadata["thresholds"]
    median_checks = {
        "total_latency": paired_ratios["candidate_total_latency_ratio"]["median"]
        <= thresholds["max_total_latency_ratio"],
        "prefill_latency": paired_ratios["candidate_prefill_latency_ratio"]["median"]
        <= thresholds["max_prefill_latency_ratio"],
        "decode_latency": paired_ratios["candidate_decode_latency_ratio"]["median"]
        <= thresholds["max_decode_latency_ratio"],
        "decode_throughput": paired_ratios["candidate_decode_throughput_ratio"]["median"]
        >= thresholds["min_decode_throughput_ratio"],
        "target_wins": target_wins >= thresholds["min_target_wins"],
        "cv": not cv_violations,
    }
    checks.update(median_checks)
    for name, passed in median_checks.items():
        if not passed:
            failures.append(f"A/B gate failed: {name}")
    result.update(
        {
            "pairs": pairs,
            "metrics": metrics,
            "paired_ratios": paired_ratios,
            "target_phase": metadata["target_phase"],
            "target_wins": target_wins,
            "cv_violations": cv_violations,
            "thresholds": thresholds,
            "failures": failures,
            "passed": not failures,
        }
    )
    return result


def _run_invocation(
    metadata: dict[str, Any],
    invocation: dict[str, Any],
    prompt: str,
    root: Path,
) -> None:
    variant = invocation["variant"]
    # The recorded effective map includes explicit nulls for the other variant's
    # controls, so an inherited candidate flag cannot leak into the baseline.
    common: dict[str, str | None] = {}
    variant_env = metadata[f"effective_{variant}_env"]
    environment = _effective_environment(
        os.environ.copy(),
        common,
        variant_env,
        invocation["stage_timing"],
        metadata[f"{variant}_route_profile"],
    )
    binary = Path(metadata["antfly_bin"])
    command = [str(binary)]
    if binary.name == "antfly":
        command.append("inference")
    command.extend(
        (
            "generate",
            metadata["model"],
            prompt,
            "--backend",
            "metal",
            "--mode",
            "compiled",
            "--compiled-target",
            "whole-model",
            "--max-tokens",
            str(invocation["output_tokens"]),
            "--temperature",
            "0",
            "--raw-prompt",
            "--ignore-eos",
            "--cache-dtype",
            metadata["cache_dtype"],
            "--print-token-count",
            "--print-finish-reason",
            "--print-token-ids",
            "--print-prompt-token-ids",
            "--print-timing",
            "--json-timing",
            str(root / f"{invocation['label']}.json"),
        )
    )
    log_path = root / f"{invocation['label']}.log"
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
            f"{invocation['label']} exited {completed.returncode}; see {log_path}"
        )


def _validate_run_args(args: argparse.Namespace) -> None:
    for name in ("runs", "output_tokens", "expected_prompt_tokens", "warmup_output_tokens"):
        if getattr(args, name) <= 0:
            raise BenchmarkContractError(f"--{name.replace('_', '-')} must be positive")
    if args.output_tokens < 2 or args.warmup_output_tokens < 2:
        raise BenchmarkContractError("output and warmup token counts must be at least two")
    for name in ("warmups", "stage_timing_runs", "cooldown_seconds"):
        if getattr(args, name) < 0:
            raise BenchmarkContractError(f"--{name.replace('_', '-')} must be non-negative")
    if args.mode == "determinism":
        if args.runs < 3:
            raise BenchmarkContractError("determinism mode requires at least three runs")
        if args.stage_timing_runs != 0:
            raise BenchmarkContractError("determinism mode does not accept stage-timing runs")
    if args.mode == "stage":
        if args.stage_timing_runs <= 0:
            raise BenchmarkContractError("stage mode requires --stage-timing-runs > 0")
    if args.stage_timing_runs > 0 and args.output_tokens <= STAGE_TIMING_SAMPLING["decode_start"]:
        raise BenchmarkContractError(
            "stage timing requires enough output tokens to sample at least one decode frame"
        )
    if args.mode == "paired" and (args.runs < 2 or args.runs % 2 != 0):
        raise BenchmarkContractError("paired mode requires a positive even number of pairs")
    for name in ("expected_token_ids_sha256", "expected_prompt_token_ids_sha256"):
        value = getattr(args, name).lower()
        if _HEX_SHA256.fullmatch(value) is None:
            raise BenchmarkContractError(f"--{name.replace('_', '-')} must be a SHA-256")
        setattr(args, name, value)
    if args.expected_warmup_token_ids_sha256:
        args.expected_warmup_token_ids_sha256 = args.expected_warmup_token_ids_sha256.lower()
        if _HEX_SHA256.fullmatch(args.expected_warmup_token_ids_sha256) is None:
            raise BenchmarkContractError("--expected-warmup-token-ids-sha256 must be a SHA-256")
    if not (0 < args.max_cv < 1):
        raise BenchmarkContractError("--max-cv must be between zero and one")
    for name in (
        "max_total_latency_ratio",
        "max_prefill_latency_ratio",
        "max_decode_latency_ratio",
        "min_decode_throughput_ratio",
    ):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            raise BenchmarkContractError(f"--{name.replace('_', '-')} must be positive and finite")
    if args.mode == "paired" and not 0 <= args.min_target_wins <= args.runs:
        raise BenchmarkContractError("--min-target-wins must be between zero and --runs")


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    _validate_run_args(args)
    repo = SCRIPT_DIR.parents[3]
    root = args.out_dir.resolve()
    try:
        root.relative_to(repo.resolve())
    except ValueError:
        pass
    else:
        raise BenchmarkContractError("--out-dir must be outside the repository")
    if root.exists() and any(root.iterdir()):
        raise BenchmarkContractError(f"--out-dir must not already contain files: {root}")

    model = args.model.resolve()
    if not model.exists():
        raise BenchmarkContractError(f"model path does not exist: {model}")
    gguf = _resolve_gguf(model, args.gguf)
    binary = args.antfly_bin.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise BenchmarkContractError(f"Antfly binary is not executable: {binary}")
    common = _merge_env_json(args.common_env, args.common_env_json, "common")
    baseline = _merge_env_json(args.baseline_env, args.baseline_env_json, "baseline")
    candidate = _merge_env_json(args.candidate_env, args.candidate_env_json, "candidate")
    _validate_variant_environments(
        common,
        baseline,
        candidate,
        args.baseline_route_profile,
        args.candidate_route_profile,
    )
    prompt = args.prompt if args.prompt is not None else _default_prompt(args.prompt_repeat)
    plan = _invocation_plan(
        args.mode,
        args.warmups,
        args.runs,
        args.stage_timing_runs,
        args.output_tokens,
        args.warmup_output_tokens,
    )
    metadata = {
        "schema": METADATA_SCHEMA,
        "experiment_id": args.experiment_id,
        "mode": args.mode,
        "target_phase": args.target_phase,
        "repo_root": str(repo),
        **_git_provenance(repo),
        "runner_sha256": _file_sha256(Path(__file__).resolve()),
        "shared_parser_sha256": _file_sha256(SHARED_PARSER),
        "host": platform.platform(),
        "machine": platform.machine(),
        # Machine-identity + thermal ledger (GEMMA4_PERF_PLAN.md M0.4): the
        # roofline differs 2.3x between base M4 (120 GB/s) and M4 Pro
        # (273 GB/s); summaries from different chips must never be compared.
        "chip": _sysctl("machdep.cpu.brand_string"),
        "hw_model": _sysctl("hw.model"),
        "memsize_bytes": _sysctl("hw.memsize"),
        "nominal_gb_s": _nominal_bandwidth_gb_s(_sysctl("machdep.cpu.brand_string")),
        "thermal_speed_limit_pct_start": _thermal_speed_limit_pct(),
        "expected_metal_device": args.expected_metal_device,
        "model": str(model),
        "gguf": str(gguf),
        "gguf_sha256": _file_sha256(gguf),
        "antfly_bin": str(binary),
        "antfly_binary_sha256": _file_sha256(binary),
        "prompt_sha256": _sha256_text(prompt),
        "prompt_repeat": args.prompt_repeat,
        "expected_prompt_tokens": args.expected_prompt_tokens,
        "expected_prompt_token_ids_sha256": args.expected_prompt_token_ids_sha256,
        "output_tokens": args.output_tokens,
        "expected_token_ids_sha256": args.expected_token_ids_sha256,
        "warmups": args.warmups,
        "warmup_output_tokens": args.warmup_output_tokens,
        "expected_warmup_token_ids_sha256": args.expected_warmup_token_ids_sha256 or None,
        "runs": args.runs,
        "stage_timing_runs": args.stage_timing_runs,
        "cooldown_seconds": args.cooldown_seconds,
        "cache_dtype": args.cache_dtype,
        "baseline_name": args.baseline_name,
        "candidate_name": args.candidate_name,
        "baseline_route_profile": args.baseline_route_profile,
        "candidate_route_profile": args.candidate_route_profile,
        "baseline_gqa_split_variants": _expected_gqa_split_variants(common, baseline),
        "candidate_gqa_split_variants": _expected_gqa_split_variants(common, candidate),
        "baseline_q4_mmv_workloads": _expected_q4_mmv_workload_variants(common, baseline),
        "candidate_q4_mmv_workloads": _expected_q4_mmv_workload_variants(common, candidate),
        "expected_q4_mmv_variant": args.expected_q4_mmv_variant,
        "expected_pair_mmv_variant": args.expected_pair_mmv_variant,
        "expected_pair_mm_variant": args.expected_pair_mm_variant,
        "common_env": dict(sorted(common.items())),
        "baseline_env": dict(sorted(baseline.items())),
        "candidate_env": dict(sorted(candidate.items())),
        "effective_baseline_env": _effective_env_record(
            common,
            baseline,
            baseline.keys() | candidate.keys(),
            args.baseline_route_profile,
        ),
        "effective_candidate_env": _effective_env_record(
            common,
            candidate,
            baseline.keys() | candidate.keys(),
            args.candidate_route_profile,
        ),
        "invocations": plan,
        "thresholds": {
            "max_total_latency_ratio": args.max_total_latency_ratio,
            "max_prefill_latency_ratio": args.max_prefill_latency_ratio,
            "max_decode_latency_ratio": args.max_decode_latency_ratio,
            "min_decode_throughput_ratio": args.min_decode_throughput_ratio,
            "min_target_wins": args.min_target_wins,
            "max_cv": args.max_cv,
        },
        "stage_timing_contract": {
            "env": "TERMITE_METAL_STAGE_TIMING=1",
            "log_prefix": "metal_stage_timing_ns:",
            "scope": STAGE_TIMING_SCOPE,
            "excluded_from_performance_statistics": True,
            "sampling": STAGE_TIMING_SAMPLING,
        },
        "gqa_split_schedule_contract": {
            "log_prefix": "metal_decode_gqa_split_schedule:",
            "trace_environment": f"{GQA_SPLIT_TRACE_ENV}=1",
            "variant_environments": GQA_SPLIT_VARIANT_ENV,
            "variants": list(GQA_SPLIT_VARIANTS),
            "default_variant": "s32",
            "final_snapshot_wins": True,
        },
        "q4_mmv_workload_contract": {
            "log_prefix": "metal-q4-0-mmv",
            "trace_environment": f"{Q4_MMV_TRACE_ENV}=1",
            "variant_environments": Q4_MMV_WORKLOAD_ENV,
            "variants": list(Q4_MMV_VARIANTS),
            "dispatches_per_decode_frame": Q4_MMV_WORKLOAD_DISPATCHES_PER_FRAME,
            "global_override_precedence": True,
        },
        "environment_isolation_contract": {
            "clear_inherited_prefixes": list(POLICY_ENV_PREFIXES),
            "reapply_only_explicit_maps": True,
            "runner_owned": sorted(RUNNER_OWNED_ENV_NAMES),
            "runner_owned_values": {
                "TERMITE_GEN_STAGE_DEBUG": "1",
                "TERMITE_METAL_STAGE_TIMING": "1 when invocation.stage_timing is true, otherwise 0",
                GQA_SPLIT_TRACE_ENV: "1 only for gqa_split_schedule route profile, otherwise 0",
                Q4_MMV_TRACE_ENV: "1 only for q4_mmv_workload route profile, otherwise 0",
            },
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / "prompt.txt").write_text(prompt)
    _write_json(root / "metadata.json", metadata)
    for invocation in plan:
        _run_invocation(metadata, invocation, prompt, root)
        if args.cooldown_seconds:
            time.sleep(args.cooldown_seconds)
    summary = build_summary(root)
    _write_json(root / "summary.json", summary)
    return summary


def _positive_env_int(name: str, default: int, *, allow_zero: bool = False) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{name} must be an integer") from exc
    if value < 0 or (value == 0 and not allow_zero):
        raise argparse.ArgumentTypeError(f"{name} has invalid value {value}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run fresh-process A/B or determinism samples")
    run.add_argument("--out-dir", type=Path, required=True)
    run.add_argument("--experiment-id", required=True)
    run.add_argument("--mode", choices=("paired", "determinism", "stage"), default="paired")
    run.add_argument("--target-phase", choices=("total", "prefill", "decode"), default="decode")
    run.add_argument("--model", type=Path, required=True)
    run.add_argument("--gguf", type=Path)
    run.add_argument("--antfly-bin", type=Path, required=True)
    run.add_argument("--prompt")
    run.add_argument("--prompt-repeat", type=int, default=36)
    run.add_argument("--expected-prompt-tokens", type=int, required=True)
    run.add_argument("--expected-prompt-token-ids-sha256", required=True)
    run.add_argument(
        "--output-tokens",
        type=int,
        default=_positive_env_int("OUTPUT_TOKENS", 128),
    )
    run.add_argument("--expected-token-ids-sha256", required=True)
    run.add_argument(
        "--runs", type=int, default=_positive_env_int("RUNS", 6)
    )
    run.add_argument(
        "--warmups", type=int, default=_positive_env_int("WARMUPS", 1, allow_zero=True)
    )
    run.add_argument(
        "--warmup-output-tokens",
        type=int,
        default=_positive_env_int("WARMUP_OUTPUT_TOKENS", 4),
    )
    run.add_argument("--expected-warmup-token-ids-sha256", default="")
    run.add_argument(
        "--stage-timing-runs",
        type=int,
        default=_positive_env_int("STAGE_TIMING_RUNS", 0, allow_zero=True),
    )
    run.add_argument(
        "--cooldown-seconds",
        type=int,
        default=_positive_env_int("COOLDOWN_SECONDS", 15, allow_zero=True),
    )
    run.add_argument("--cache-dtype", default="f16")
    run.add_argument("--baseline-name", default="baseline")
    run.add_argument("--candidate-name", default="candidate")
    run.add_argument("--baseline-route-profile", choices=ROUTE_PROFILES, default="split_ffn")
    run.add_argument("--candidate-route-profile", choices=ROUTE_PROFILES, default="split_ffn")
    run.add_argument("--expected-q4-mmv-variant", default="nr4-nsg2")
    run.add_argument("--expected-pair-mmv-variant", default="nr4-nsg2")
    run.add_argument("--expected-pair-mm-variant", default="m32-n64-tail")
    run.add_argument("--common-env", action="append", default=[])
    run.add_argument("--baseline-env", action="append", default=[])
    run.add_argument("--candidate-env", action="append", default=[])
    run.add_argument("--common-env-json", default=os.environ.get("COMMON_ENV_JSON", ""))
    run.add_argument("--baseline-env-json", default=os.environ.get("BASELINE_ENV_JSON", ""))
    run.add_argument("--candidate-env-json", default=os.environ.get("CANDIDATE_ENV_JSON", ""))
    run.add_argument("--expected-metal-device", default="Apple M4")
    run.add_argument("--max-total-latency-ratio", type=float, default=0.995)
    run.add_argument("--max-prefill-latency-ratio", type=float, default=1.005)
    run.add_argument("--max-decode-latency-ratio", type=float, default=0.99)
    run.add_argument("--min-decode-throughput-ratio", type=float, default=1.0)
    run.add_argument("--min-target-wins", type=int, default=5)
    run.add_argument("--max-cv", type=float, default=0.03)

    summarize = subparsers.add_parser("summarize", help="revalidate and summarize raw artifacts")
    summarize.add_argument("--out-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.command == "run":
            summary = run_experiment(args)
        else:
            summary = build_summary(args.out_dir.resolve())
            _write_json(args.out_dir.resolve() / "summary.json", summary)
    except (BenchmarkContractError, OSError, subprocess.SubprocessError) as exc:
        raise SystemExit(str(exc)) from exc
    if summary["mode"] == "paired":
        ratios = summary["paired_ratios"]
        print(
            f"Gemma4 Metal A/B {summary['experiment_id']}: "
            f"total={ratios['candidate_total_latency_ratio']['median']:.4f} "
            f"prefill={ratios['candidate_prefill_latency_ratio']['median']:.4f} "
            f"decode={ratios['candidate_decode_latency_ratio']['median']:.4f} "
            f"wins={summary['target_wins']}/{summary['metadata']['runs']} "
            f"passed={summary['passed']}"
        )
    elif summary["mode"] == "determinism":
        print(
            f"Gemma4 Metal determinism {summary['experiment_id']}: "
            f"runs={len(summary['determinism_samples'])} "
            f"digests={len(summary['unique_output_token_digests'])} "
            f"passed={summary['passed']}"
        )
    else:
        print(
            f"Gemma4 Metal stage timing {summary['experiment_id']}: "
            f"runs={len(summary['stage_timing_samples'])} passed={summary['passed']}"
        )
    if not summary["passed"]:
        raise SystemExit("\n".join(summary["failures"]))


if __name__ == "__main__":
    main()
