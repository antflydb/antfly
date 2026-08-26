#!/usr/bin/env python3
"""Fail-closed Gemma4 Metal parity gate against llama.cpp.

The A4B lane measures three fresh-process Antfly variants plus matched
llama.cpp runs: the default compiled control, the prepared whole-model
candidate, and the candidate's specialized-ID rollback. Optional E2B/E4B
controls pair the A4B feature umbrella with its specialized-kernel rollback
and also run the same llama.cpp comparator. This separates executor gains,
kernel gains, dense-model non-regression, and llama.cpp parity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
from typing import Any


LLAMA_EVAL = re.compile(
    r"eval time\s*=\s*([0-9.]+) ms\s*/\s*(\d+) runs\s*"
    r"\(\s*([0-9.]+) ms per token,\s*([0-9.]+) tokens per second\)"
)
TOKEN_IDS = re.compile(r"^token_ids:\s*(.*?)\s*$", re.MULTILINE)
MAX_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE)
POLICY_PREFIXES = ("TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_")
FALSE_ENV_VALUES = ("", "0", "false", "no", "off")
FORBIDDEN = (
    "CompactMoeChunkExecutionFailed",
    "GENERATION_FAILED",
    "mapped_moe_failure",
    "metal-runtime frame fallback",
    "metal-prepared-frame: fallback",
)

DEFAULT_PROMPTS = (
    "<|turn>user\nExplain in two concise sentences why LSM trees are useful for write-heavy databases.<turn|>\n<|turn>model\n<|channel>final\n<channel|>",
    "<|turn>user\nGive three brief practical tips for profiling a GPU inference loop.<turn|>\n<|turn>model\n<|channel>final\n<channel|>",
    "<|turn>user\nIn one paragraph, compare a B-tree with a log-structured merge tree.<turn|>\n<|turn>model\n<|channel>final\n<channel|>",
)


class GateError(RuntimeError):
    pass


def clean_environment(extra: dict[str, str]) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(POLICY_PREFIXES)
    }
    env.update(extra)
    return env


def env_flag_enabled(environment: dict[str, str], name: str) -> bool:
    return environment.get(name, "").lower() not in FALSE_ENV_VALUES


def token_contract(log: str, path: Path) -> tuple[int, str]:
    matches = list(TOKEN_IDS.finditer(log))
    if not matches:
        raise GateError(f"token_ids marker missing: {path}")
    text = matches[-1].group(1).strip()
    try:
        count = len([int(value) for value in text.split()])
    except ValueError as exc:
        raise GateError(f"invalid token_ids marker: {path}") from exc
    return count, hashlib.sha256(text.encode()).hexdigest()


def run_antfly(
    args: argparse.Namespace,
    *,
    label: str,
    model: Path,
    prompt: str,
    a4b: bool,
    specialized: bool,
    prepared_a4b: bool = False,
    exercise_a4b_feature: bool = False,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    json_path = args.out_dir / f"{label}.json"
    log_path = args.out_dir / f"{label}.log"
    env_extra = {
        "TERMITE_GEN_STAGE_DEBUG": "1",
        "ANTFLY_INFERENCE_JSON_TOKEN_IDS": "1",
    }
    if a4b or exercise_a4b_feature:
        env_extra["TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH"] = "1"
        if not specialized:
            env_extra["TERMITE_METAL_DISABLE_A4B_SPECIALIZED_ID"] = "1"
    if a4b and prepared_a4b:
        env_extra["TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE"] = "1"
    if extra_env:
        env_extra.update(extra_env)
    command = [
        "/usr/bin/time",
        "-l",
        str(args.antfly_bin),
        "generate",
        str(model),
        prompt,
        "--backend",
        "metal",
        "--mode",
        "compiled",
        "--compiled-target",
        "whole-model",
        "--cache-dtype",
        "f16",
        "--raw-prompt",
        "--temperature",
        "0",
        "--ignore-eos",
        "--max-tokens",
        str(args.output_tokens if a4b else args.control_output_tokens),
        "--print-token-ids",
        "--json-timing",
        str(json_path),
    ]
    if a4b:
        budget_args = [
            "--a4b-residency-mode",
            "resident",
            "--a4b-memory-budget-mb",
            str(args.budget_mb),
            "--backend-budget-mb",
            str(args.budget_mb),
            "--combined-budget-mb",
            str(args.budget_mb),
        ]
        command[command.index("--raw-prompt"):command.index("--raw-prompt")] = budget_args
    with log_path.open("w") as output:
        completed = subprocess.run(
            command,
            env=clean_environment(env_extra),
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise GateError(f"Antfly sample {label} exited {completed.returncode}: {log_path}")
    try:
        payload = json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"invalid Antfly JSON {json_path}: {exc}") from exc
    log = log_path.read_text(errors="replace")
    for marker in FORBIDDEN:
        if marker in log:
            raise GateError(f"forbidden marker {marker!r}: {log_path}")
    if payload.get("backend") != "metal" or payload.get("finish_reason") != "length":
        raise GateError(f"Antfly sample did not complete on Metal: {json_path}")
    timing = payload.get("timing_ms")
    if not isinstance(timing, dict):
        raise GateError(f"timing_ms missing: {json_path}")
    decode_ms = float(timing.get("decode_inner") or 0)
    output_tokens = int(payload.get("tokens") or 0)
    if decode_ms <= 0 or output_tokens < 2:
        raise GateError(f"invalid decode timing/token count: {json_path}")
    log_count, token_sha = token_contract(log, log_path)
    if log_count != output_tokens:
        raise GateError(f"JSON/log token count differs: {json_path}")
    if payload.get("token_ids") is not None and len(payload["token_ids"]) != log_count:
        raise GateError(f"JSON token_ids count differs from log: {json_path}")
    rss_match = MAX_RSS.search(log)
    if rss_match is None:
        raise GateError(f"maximum RSS marker missing: {log_path}")
    rss_mb = int(rss_match.group(1)) / (1024 * 1024)
    if a4b and rss_mb > args.max_a4b_rss_mb:
        raise GateError(
            f"A4B RSS {rss_mb:.1f} MiB exceeds {args.max_a4b_rss_mb:.1f}: {log_path}"
        )
    if a4b and specialized and "metal_a4b_specialized_id: enabled=1" not in log:
        raise GateError(f"specialized A4B marker missing: {log_path}")
    if a4b and not specialized and "metal_a4b_specialized_id: enabled=1" in log:
        raise GateError(f"rollback sample used specialized A4B kernel: {log_path}")
    if a4b:
        route_tg_disabled = env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_ROUTE_SELECT_TG")
        route_tg_marker = "metal_a4b_route_select_tg: enabled=1"
        if route_tg_disabled and route_tg_marker in log:
            raise GateError(f"route-select rollback used SIMD-group kernel: {log_path}")
        if not route_tg_disabled and route_tg_marker not in log:
            raise GateError(f"A4B SIMD-group route-select marker missing: {log_path}")
        lm_head_nbodd_disabled = env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_LM_HEAD_NBODD")
        lm_head_nbodd_marker = "metal_a4b_lm_head_nbodd: enabled=1"
        if lm_head_nbodd_disabled and lm_head_nbodd_marker in log:
            raise GateError(f"LM-head rollback used nb-odd kernel: {log_path}")
        if not lm_head_nbodd_disabled and lm_head_nbodd_marker not in log:
            raise GateError(f"A4B LM-head nb-odd marker missing: {log_path}")
        argmax_tg_disabled = env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_ARGMAX_TG")
        argmax_tg_marker = "metal_a4b_argmax_tg: enabled=1"
        if argmax_tg_disabled and argmax_tg_marker in log:
            raise GateError(f"argmax rollback used threadgroup reducer: {log_path}")
        if not argmax_tg_disabled and argmax_tg_marker not in log:
            raise GateError(f"A4B argmax threadgroup marker missing: {log_path}")
    if exercise_a4b_feature and not a4b and "metal_a4b_specialized_id: enabled=1" in log:
        raise GateError(f"dense control selected an A4B-only kernel: {log_path}")
    if a4b:
        metal = payload.get("metal")
        if not isinstance(metal, dict):
            raise GateError(f"Metal telemetry missing: {json_path}")
        resident = metal.get("resident_mapped")
        residency = metal.get("residency")
        q4_policy = metal.get("q4_0_policy")
        frame_fallbacks = metal.get("frame_fallbacks")
        prepared_frame = metal.get("prepared_frame")
        if not all(
            isinstance(value, dict)
            for value in (resident, residency, q4_policy, frame_fallbacks, prepared_frame)
        ):
            raise GateError(f"A4B route telemetry missing: {json_path}")
        dispatches = resident.get("dispatches")
        model_buffer = resident.get("model_buffer")
        residency_set = resident.get("residency_set")
        if not all(isinstance(value, dict) for value in (dispatches, model_buffer, residency_set)):
            raise GateError(f"A4B resident telemetry incomplete: {json_path}")
        down = int(dispatches.get("down") or 0)
        reduce = int(dispatches.get("reduce") or 0)
        fused = int(dispatches.get("fused_gate_up") or 0)
        if down <= 0 or reduce != down or fused != down * 2:
            raise GateError(f"A4B mapped dispatch contract failed: {json_path}")
        if int(model_buffer.get("prepare_successes") or 0) != 1 or int(model_buffer.get("prepare_failures") or 0):
            raise GateError(f"A4B model-wide mapped buffer was not admitted: {json_path}")
        if int(residency_set.get("allocated_bytes") or 0) < 14_000_000_000:
            raise GateError(f"A4B residency set is not model-wide: {json_path}")
        if int(residency.get("runtime_mapped_fallbacks") or 0) or int(residency.get("runtime_mapped_failures") or 0):
            raise GateError(f"A4B mapped weights fell back: {json_path}")
        if int(q4_policy.get("mmv_variant_fallbacks") or 0):
            raise GateError(f"Q4_0 schedule fell back: {json_path}")
        high_memory_fast_path = (
            env_flag_enabled(env_extra, "TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH")
            and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_HIGH_MEMORY_FAST_PATH")
        )
        pipelined_decode = prepared_a4b and (
            high_memory_fast_path
            or env_flag_enabled(env_extra, "TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME")
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME")
        expected_prepared_frames = (
            output_tokens if pipelined_decode else output_tokens - 1 if prepared_a4b else 0
        )
        if int(prepared_frame.get("fast_path") or 0) != expected_prepared_frames:
            raise GateError(
                "A4B prepared decode coverage differs from the selected executor "
                f"({prepared_frame.get('fast_path')} != {expected_prepared_frames}): {json_path}"
            )
        if int(prepared_frame.get("fallback") or 0):
            raise GateError(f"A4B prepared decode fell back: {json_path}")
        if int(frame_fallbacks.get("decode_success") or 0) != expected_prepared_frames:
            raise GateError(f"A4B successful decode-frame count differs from coverage: {json_path}")
        pipelined_marker = "metal_pipelined_decode_frame: enabled=1 owner=executor"
        if pipelined_decode and pipelined_marker not in log:
            raise GateError(f"A4B pipelined decode marker missing: {log_path}")
        if not pipelined_decode and pipelined_marker in log:
            raise GateError(f"A4B non-pipelined lane used pipelined decode: {log_path}")
        register_selector = (
            high_memory_fast_path
            or env_flag_enabled(env_extra, "TERMITE_METAL_ENABLE_A4B_ROUTE_SELECT_REGISTER")
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_ROUTE_SELECT_REGISTER")
        register_marker = "metal_a4b_route_select_register: enabled=1"
        if register_selector and register_marker not in log:
            raise GateError(f"A4B register route-selector marker missing: {log_path}")
        if not register_selector and register_marker in log:
            raise GateError(f"A4B register route-selector rollback was not honored: {log_path}")
        lm_head_nr4_nsg1 = (
            high_memory_fast_path
            or env_flag_enabled(env_extra, "TERMITE_METAL_ENABLE_A4B_LM_HEAD_NR4_NSG1")
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_LM_HEAD_NR4_NSG1")
        lm_head_nr4_marker = "metal_a4b_lm_head_nr4_nsg1: enabled=1"
        if lm_head_nr4_nsg1 and lm_head_nr4_marker not in log:
            raise GateError(f"A4B NR4/NSG1 LM-head marker missing: {log_path}")
        if not lm_head_nr4_nsg1 and lm_head_nr4_marker in log:
            raise GateError(f"A4B NR4/NSG1 LM-head rollback was not honored: {log_path}")
        concurrent_hazard = prepared_a4b and (
            high_memory_fast_path
            or env_flag_enabled(env_extra, "TERMITE_METAL_ENABLE_A4B_CONCURRENT_HAZARD")
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_A4B_CONCURRENT_HAZARD")
        concurrent_marker = "metal_a4b_concurrent_hazard: enabled=1"
        if concurrent_hazard and concurrent_marker not in log:
            raise GateError(f"A4B concurrent-hazard marker missing: {log_path}")
        if not concurrent_hazard and concurrent_marker in log:
            raise GateError(f"A4B concurrent-hazard rollback was not honored: {log_path}")
        split_frame_scratch = (
            high_memory_fast_path
            or env_flag_enabled(
                env_extra, "TERMITE_METAL_ENABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH"
            )
        ) and not env_flag_enabled(
            env_extra, "TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH"
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT")
        split_frame_scratch_marker = (
            "metal_a4b_decode_gqa_split_frame_scratch: enabled=1 slots=2"
        )
        if split_frame_scratch and split_frame_scratch_marker not in log:
            raise GateError(f"A4B frame-owned split-GQA scratch marker missing: {log_path}")
        if not split_frame_scratch and split_frame_scratch_marker in log:
            raise GateError(f"A4B frame-owned split-GQA scratch rollback was not honored: {log_path}")
        flash_prefill_hd256 = env_flag_enabled(
            env_extra, "TERMITE_METAL_ENABLE_A4B_FLASH_PREFILL_HD256"
        ) and not env_flag_enabled(
            env_extra, "TERMITE_METAL_DISABLE_A4B_FLASH_PREFILL_HD256"
        ) and not env_flag_enabled(env_extra, "TERMITE_METAL_DISABLE_FLASH_PREFILL_GENERATED")
        flash_prefill_marker = "metal_a4b_flash_prefill_hd256: enabled=1"
        if flash_prefill_hd256 and flash_prefill_marker not in log:
            raise GateError(f"A4B local HD256 flash-prefill marker missing: {log_path}")
        if not flash_prefill_hd256 and flash_prefill_marker in log:
            raise GateError(f"A4B local HD256 flash-prefill rollback was not honored: {log_path}")
        attention_dispatch = metal.get("attention_dispatch")
        if not isinstance(attention_dispatch, dict):
            raise GateError(f"A4B attention-dispatch telemetry missing: {json_path}")
        generated_flash_calls = int(
            attention_dispatch.get("generated_flash_prefill") or 0
        )
        expected_flash_calls = 25 if flash_prefill_hd256 else 0
        if generated_flash_calls != expected_flash_calls:
            raise GateError(
                "A4B local flash-prefill coverage differs from the 25 local layers "
                f"({generated_flash_calls} != {expected_flash_calls}): {json_path}"
            )
        if int(attention_dispatch.get("generated_flash_prefill_hd512") or 0) != 5:
            raise GateError(f"A4B global HD512 flash-prefill coverage is not 5: {json_path}")
        split_calls = int(attention_dispatch.get("decode_gqa_split") or 0)
        if prepared_a4b and split_frame_scratch and split_calls > 0:
            expected_split_calls = output_tokens * 30
            expected_paged_calls = 0 if flash_prefill_hd256 else 25
            paged_calls = int(attention_dispatch.get("paged_1x") or 0)
            if split_calls != expected_split_calls or paged_calls != expected_paged_calls:
                raise GateError(
                    "A4B frame-owned split-GQA coverage is incomplete "
                    f"(split/paged={split_calls}/{paged_calls}, expected "
                    f"{expected_split_calls}/{expected_paged_calls}): {json_path}"
                )
        expected_executor_marker = (
            "generate-setup: live whole-model executor handled request"
            if prepared_a4b
            else "generate-setup: live whole-model executor skipped"
        )
        if expected_executor_marker not in log:
            raise GateError(f"A4B executor marker missing: {log_path}")
        if any(int(value or 0) for key, value in frame_fallbacks.items() if key.endswith("fallback")):
            raise GateError(f"Metal frame execution fell back: {json_path}")
    return {
        "engine": "antfly",
        "label": label,
        "decode_ms": decode_ms,
        "decode_runs": output_tokens - 1,
        "decode_tok_s": (output_tokens - 1) * 1000.0 / decode_ms,
        "output_tokens": output_tokens,
        "token_ids_sha256": token_sha,
        "max_rss_mb": rss_mb,
        "specialized": specialized,
        "prepared_a4b": prepared_a4b,
    }


def run_llama(
    args: argparse.Namespace,
    *,
    label: str,
    model: Path,
    prompt: str,
    output_tokens: int,
) -> dict[str, Any]:
    log_path = args.out_dir / f"{label}.log"
    command = [
        "/usr/bin/time",
        "-l",
        str(args.llama_bin),
        "-m",
        str(model),
        "-p",
        prompt,
        "-n",
        str(output_tokens),
        "--temp",
        "0",
        "--ignore-eos",
        "--no-display-prompt",
        "--no-conversation",
        "-ngl",
        "999",
        "-c",
        "4096",
        "-b",
        "512",
        "-ub",
        "512",
        "-fa",
        "on",
        "-ctk",
        "f16",
        "-ctv",
        "f16",
        "--seed",
        "1",
        "--perf",
    ]
    with log_path.open("w") as output:
        completed = subprocess.run(
            command,
            env=clean_environment({}),
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise GateError(f"llama.cpp sample {label} exited {completed.returncode}: {log_path}")
    log = log_path.read_text(errors="replace")
    matches = list(LLAMA_EVAL.finditer(log))
    if not matches:
        raise GateError(f"llama.cpp eval timing missing: {log_path}")
    match = matches[-1]
    decode_ms = float(match.group(1))
    decode_runs = int(match.group(2))
    decode_tok_s = float(match.group(4))
    if decode_runs != output_tokens - 1 or decode_ms <= 0 or decode_tok_s <= 0:
        raise GateError(f"llama.cpp decode contract mismatch: {log_path}")
    rss_match = MAX_RSS.search(log)
    return {
        "engine": "llama.cpp",
        "label": label,
        "decode_ms": decode_ms,
        "decode_runs": decode_runs,
        "decode_tok_s": decode_tok_s,
        "max_rss_mb": int(rss_match.group(1)) / (1024 * 1024) if rss_match else None,
    }


def median(values: list[float]) -> float:
    if not values:
        raise GateError("cannot summarize an empty sample set")
    return statistics.median(values)


def parse_control(value: str) -> tuple[str, Path]:
    name, separator, path = value.partition("=")
    if not separator or not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise argparse.ArgumentTypeError("controls must be NAME=/path/to/model.gguf")
    return name.lower(), Path(path)


def parse_policy_env(value: str) -> tuple[str, str]:
    name, separator, setting = value.partition("=")
    if not separator or not re.fullmatch(
        r"(?:TERMITE|ANTFLY_GEMMA4|ANTFLY_INFERENCE)_[A-Za-z0-9_]+",
        name,
    ):
        raise argparse.ArgumentTypeError("policy environment entries must be NAME=VALUE")
    if any(character in setting for character in "\n\r\0"):
        raise argparse.ArgumentTypeError(
            "policy environment values cannot contain control characters"
        )
    return name, setting


def coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if mean > 0 else float("inf")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly-bin", type=Path, required=True)
    parser.add_argument("--llama-bin", type=Path, required=True)
    parser.add_argument("--a4b-model", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--prompt", action="append", dest="prompts")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output-tokens", type=int, default=128)
    parser.add_argument("--control-output-tokens", type=int, default=64)
    parser.add_argument("--budget-mb", type=int, default=16384)
    parser.add_argument("--max-a4b-rss-mb", type=float, default=18432)
    parser.add_argument("--min-median-parity", type=float, default=0.95)
    parser.add_argument("--min-prompt-parity", type=float, default=0.90)
    parser.add_argument("--min-prepared-ratio", type=float, default=0.99)
    parser.add_argument("--min-rollback-ratio", type=float, default=0.99)
    parser.add_argument("--min-control-parity", type=float, default=0.90)
    parser.add_argument("--min-control-rollback-ratio", type=float, default=0.99)
    parser.add_argument("--candidate-env", action="append", type=parse_policy_env, default=[])
    parser.add_argument("--rollback-env", action="append", type=parse_policy_env, default=[])
    parser.add_argument("--rollback-specialized", action="store_true")
    parser.add_argument("--min-target-wins", type=int, default=0)
    parser.add_argument("--max-cv", type=float, default=1.0)
    parser.add_argument("--control", action="append", type=parse_control, default=[])
    args = parser.parse_args()
    args.antfly_bin = args.antfly_bin.resolve()
    args.llama_bin = args.llama_bin.resolve()
    args.a4b_model = args.a4b_model.resolve()
    args.out_dir = args.out_dir.resolve()
    args.prompts = args.prompts or list(DEFAULT_PROMPTS)
    args.candidate_env = dict(args.candidate_env)
    args.rollback_env = dict(args.rollback_env)
    if args.runs <= 0 or args.output_tokens < 2 or args.control_output_tokens < 2:
        parser.error("run and token counts must be positive")
    if not 0 <= args.min_target_wins <= args.runs * len(args.prompts):
        parser.error("--min-target-wins must be between zero and total A4B pairs")
    if not 0 < args.max_cv <= 1:
        parser.error("--max-cv must be in (0, 1]")
    for binary in (args.antfly_bin, args.llama_bin):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"binary is not executable: {binary}")
    models = [args.a4b_model] + [path.resolve() for _, path in args.control]
    if any(not model.is_file() for model in models):
        parser.error("all models must be GGUF files")
    args.control = [(name, path.resolve()) for name, path in args.control]
    if args.out_dir.exists() and any(args.out_dir.iterdir()):
        parser.error(f"--out-dir must be empty: {args.out_dir}")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    a4b_samples: list[dict[str, Any]] = []
    prompt_ratios: list[float] = []
    for prompt_index, prompt in enumerate(args.prompts):
        baseline_values: list[float] = []
        candidate_values: list[float] = []
        rollback_values: list[float] = []
        llama_values: list[float] = []
        for run in range(1, args.runs + 1):
            variant_orders = (
                ("baseline", "rollback", "candidate"),
                ("candidate", "baseline", "rollback"),
                ("rollback", "candidate", "baseline"),
            )
            variants = variant_orders[(run - 1) % len(variant_orders)]
            pair: dict[str, dict[str, Any]] = {}
            for variant in variants:
                pair[variant] = run_antfly(
                    args,
                    label=f"a4b-p{prompt_index}-r{run}-{variant}",
                    model=args.a4b_model,
                    prompt=prompt,
                    a4b=True,
                    specialized=variant != "rollback" or args.rollback_specialized,
                    prepared_a4b=variant != "baseline",
                    extra_env=(
                        args.candidate_env
                        if variant == "candidate"
                        else args.rollback_env if variant == "rollback" else None
                    ),
                )
            llama = run_llama(
                args,
                label=f"a4b-p{prompt_index}-r{run}-llama",
                model=args.a4b_model,
                prompt=prompt,
                output_tokens=args.output_tokens,
            )
            token_hashes = {
                pair[variant]["token_ids_sha256"]
                for variant in ("baseline", "candidate", "rollback")
            }
            if len(token_hashes) != 1:
                raise GateError(
                    "baseline/candidate/rollback token mismatch for prompt "
                    f"{prompt_index} run {run}"
                )
            baseline_values.append(pair["baseline"]["decode_tok_s"])
            candidate_values.append(pair["candidate"]["decode_tok_s"])
            rollback_values.append(pair["rollback"]["decode_tok_s"])
            llama_values.append(llama["decode_tok_s"])
            a4b_samples.append(
                {
                    "prompt": prompt_index,
                    "run": run,
                    "baseline": pair["baseline"],
                    "candidate": pair["candidate"],
                    "rollback": pair["rollback"],
                    "llama": llama,
                }
            )
        prompt_ratio = median(candidate_values) / median(llama_values)
        prompt_ratios.append(prompt_ratio)
        if median(candidate_values) / median(baseline_values) < args.min_prepared_ratio:
            raise GateError(f"A4B prepared candidate regressed compiled baseline on prompt {prompt_index}")
        if median(candidate_values) / median(rollback_values) < args.min_rollback_ratio:
            raise GateError(f"A4B candidate regressed rollback on prompt {prompt_index}")

    baseline_all = [sample["baseline"]["decode_tok_s"] for sample in a4b_samples]
    candidate_all = [sample["candidate"]["decode_tok_s"] for sample in a4b_samples]
    rollback_all = [sample["rollback"]["decode_tok_s"] for sample in a4b_samples]
    llama_all = [sample["llama"]["decode_tok_s"] for sample in a4b_samples]
    a4b_parity = median(candidate_all) / median(llama_all)
    a4b_prepared_ratio = median(candidate_all) / median(baseline_all)
    a4b_rollback_ratio = median(candidate_all) / median(rollback_all)
    a4b_target_wins = sum(
        sample["candidate"]["decode_tok_s"] > sample["rollback"]["decode_tok_s"]
        for sample in a4b_samples
    )
    a4b_candidate_cv = coefficient_of_variation(candidate_all)
    a4b_rollback_cv = coefficient_of_variation(rollback_all)

    control_summaries: list[dict[str, Any]] = []
    for name, model in args.control:
        candidate_values: list[float] = []
        rollback_values: list[float] = []
        llama_values: list[float] = []
        control_samples: list[dict[str, Any]] = []
        prompt = args.prompts[0]
        for run in range(1, args.runs + 1):
            variants = ("rollback", "candidate") if run % 2 else ("candidate", "rollback")
            pair: dict[str, dict[str, Any]] = {}
            for variant in variants:
                pair[variant] = run_antfly(
                    args,
                    label=f"{name}-r{run}-{variant}",
                    model=model,
                    prompt=prompt,
                    a4b=False,
                    specialized=variant == "candidate",
                    exercise_a4b_feature=True,
                )
            llama = run_llama(
                args,
                label=f"{name}-r{run}-llama",
                model=model,
                prompt=prompt,
                output_tokens=args.control_output_tokens,
            )
            if pair["candidate"]["token_ids_sha256"] != pair["rollback"]["token_ids_sha256"]:
                raise GateError(f"{name} candidate/rollback token mismatch on run {run}")
            candidate_values.append(pair["candidate"]["decode_tok_s"])
            rollback_values.append(pair["rollback"]["decode_tok_s"])
            llama_values.append(llama["decode_tok_s"])
            control_samples.append(
                {
                    "run": run,
                    "candidate": pair["candidate"],
                    "rollback": pair["rollback"],
                    "llama": llama,
                }
            )
        parity = median(candidate_values) / median(llama_values)
        rollback_ratio = median(candidate_values) / median(rollback_values)
        control_summaries.append(
            {
                "name": name,
                "model": str(model),
                "candidate_median_tok_s": median(candidate_values),
                "rollback_median_tok_s": median(rollback_values),
                "llama_median_tok_s": median(llama_values),
                "parity": parity,
                "rollback_ratio": rollback_ratio,
                "parity_passed": parity >= args.min_control_parity,
                "no_regression_passed": rollback_ratio >= args.min_control_rollback_ratio,
                "passed": parity >= args.min_control_parity
                and rollback_ratio >= args.min_control_rollback_ratio,
                "samples": control_samples,
            }
        )

    checks = {
        "a4b_median_parity": a4b_parity >= args.min_median_parity,
        "a4b_each_prompt_parity": min(prompt_ratios) >= args.min_prompt_parity,
        "a4b_prepared_no_regression": a4b_prepared_ratio >= args.min_prepared_ratio,
        "a4b_no_rollback_regression": a4b_rollback_ratio >= args.min_rollback_ratio,
        "a4b_target_wins": a4b_target_wins >= args.min_target_wins,
        "a4b_cv": max(a4b_candidate_cv, a4b_rollback_cv) <= args.max_cv,
        "positive_controls_no_regression": all(
            control["no_regression_passed"] for control in control_summaries
        ),
        "positive_controls_llama_parity": all(
            control["parity_passed"] for control in control_summaries
        ),
    }
    summary = {
        "schema": "antfly.gemma4_metal_llamacpp_parity.v3",
        "passed": all(checks.values()),
        "checks": checks,
        "thresholds": {
            "min_median_parity": args.min_median_parity,
            "min_prompt_parity": args.min_prompt_parity,
            "min_prepared_ratio": args.min_prepared_ratio,
            "min_rollback_ratio": args.min_rollback_ratio,
            "min_control_parity": args.min_control_parity,
            "min_control_rollback_ratio": args.min_control_rollback_ratio,
            "min_target_wins": args.min_target_wins,
            "max_cv": args.max_cv,
        },
        "a4b": {
            "baseline_median_tok_s": median(baseline_all),
            "candidate_median_tok_s": median(candidate_all),
            "rollback_median_tok_s": median(rollback_all),
            "llama_median_tok_s": median(llama_all),
            "parity": a4b_parity,
            "prepared_ratio": a4b_prepared_ratio,
            "rollback_ratio": a4b_rollback_ratio,
            "target_wins": a4b_target_wins,
            "candidate_cv": a4b_candidate_cv,
            "rollback_cv": a4b_rollback_cv,
            "candidate_env": args.candidate_env,
            "rollback_env": args.rollback_env,
            "rollback_specialized": args.rollback_specialized,
            "prompt_parity": prompt_ratios,
            "samples": a4b_samples,
        },
        "controls": control_summaries,
    }
    (args.out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    print(
        f"A4B baseline={median(baseline_all):.3f} tok/s "
        f"candidate={median(candidate_all):.3f} tok/s "
        f"rollback={median(rollback_all):.3f} tok/s "
        f"llama.cpp={median(llama_all):.3f} tok/s "
        f"prepared_ratio={a4b_prepared_ratio:.4f} "
        f"rollback_ratio={a4b_rollback_ratio:.4f} "
        f"wins={a4b_target_wins}/{len(a4b_samples)} "
        f"parity={a4b_parity * 100:.2f}% passed={summary['passed']}"
    )
    for control in control_summaries:
        print(
            f"{control['name']} candidate={control['candidate_median_tok_s']:.3f} tok/s "
            f"rollback={control['rollback_median_tok_s']:.3f} tok/s "
            f"llama.cpp={control['llama_median_tok_s']:.3f} tok/s "
            f"parity={control['parity'] * 100:.2f}% "
            f"rollback_ratio={control['rollback_ratio']:.4f}"
        )
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
