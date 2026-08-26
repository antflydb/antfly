#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed Gemma 4 26B-A4B CUDA qualification benchmark.

The gate binds the exact Q4_0 artifact, Antfly executable, source checkout,
SM89 device, deterministic output tokens, resident CUDA A4B counters, and a
paired llama.cpp CUDA throughput comparator into one JSON receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from gemma4_metal_long_output import BenchmarkContractError, _token_line  # noqa: E402


SCHEMA = "antfly.gemma4_a4b_cuda_qualification.v1"
QUALIFIED_MODEL_BYTES = 14_439_363_584
QUALIFIED_MODEL_SHA256 = (
    "3eca3b8f6d7baf218a7dd6bba5fb59a56ee25fe2d567b6f5f589b4f697eca51d"
)
QUALIFIED_OUTPUT_SHA256 = (
    "d4ee583f092062e7177069de1f35a9cefbaac24848d11d09b64237bc9209b68e"
)
A4B_LOAD = re.compile(
    r"cuda_a4b:\s+resident load complete\s+layers=(\d+)\s+sources=(\d+)\s+"
    r"source_mib=(\d+)\s+workspace_mib=(\d+)\s+budget_mib=(\d+)"
)
LLAMA_DECODE = re.compile(
    r"(?:eval time|eval\s+time).*?([0-9]+(?:\.[0-9]+)?)\s+tokens per second",
    re.IGNORECASE,
)
LLAMA_CUDA_DEVICE_COUNT = re.compile(
    r"ggml_cuda_init:\s*found\s+(\d+)\s+CUDA devices?",
    re.IGNORECASE,
)
LLAMA_CUDA_DEVICE_INVENTORY = re.compile(
    r"common_param:\s+-\s+CUDA(\d+)\s*:\s*([^\r\n(]+?)\s*\(",
    re.IGNORECASE,
)
LLAMA_CUDA_DEVICE = re.compile(
    r"(?:llama_prepare_model_devices|llama_model_load_from_file_impl):[^\r\n]*"
    r"using device\s+CUDA\d+\s+\(([^)\r\n]+)\)",
    re.IGNORECASE,
)
LLAMA_GPU_OFFLOAD = re.compile(
    r"load_tensors:\s*offloaded\s+(\d+)/(\d+)\s+layers to GPU(?:\s|$)",
    re.IGNORECASE | re.MULTILINE,
)
CUDA_REPLAY_KV_BLOCK_TOKENS = 32
CUDA_REPLAY_KV_HEADROOM_TOKENS = 32


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def require_sha256(path: Path, expected: str, label: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise BenchmarkContractError(f"{label} expected SHA-256 must be 64 lowercase hex characters")
    actual = sha256_file(path)
    if actual != expected:
        raise BenchmarkContractError(f"{label} SHA-256 is {actual}, expected {expected}")
    return actual


def metric_stats(values: list[float]) -> dict[str, float]:
    if not values:
        raise BenchmarkContractError("empty benchmark sample set")
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


def cuda_replay_kv_capacity(prompt_tokens: int, output_tokens: int) -> int:
    """Reserve a block-aligned persistent graph KV window for the full sample."""
    if prompt_tokens <= 0 or output_tokens <= 0:
        raise BenchmarkContractError("prompt and output token counts must be positive")
    required = prompt_tokens + output_tokens + CUDA_REPLAY_KV_HEADROOM_TOKENS
    return (
        (required + CUDA_REPLAY_KV_BLOCK_TOKENS - 1)
        // CUDA_REPLAY_KV_BLOCK_TOKENS
        * CUDA_REPLAY_KV_BLOCK_TOKENS
    )


def exact_int(mapping: dict[str, Any], key: str, path: Path) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BenchmarkContractError(f"cuda_generate.{key} must be an integer: {path}")
    return value


def parse_antfly_sample(
    json_path: Path,
    log_path: Path,
    *,
    prompt_tokens: int,
    prompt_sha256: str,
    output_tokens: int,
    output_sha256: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"invalid Antfly JSON {json_path}: {exc}") from exc
    log = log_path.read_text(errors="replace")
    if payload.get("backend") != "cuda":
        raise BenchmarkContractError(f"sample did not report CUDA: {json_path}")
    if payload.get("tokens") != output_tokens or payload.get("finish_reason") != "length":
        raise BenchmarkContractError(f"sample was not length-limited to {output_tokens} tokens")

    output_text, output_ids = _token_line(log, "token_ids", log_path)
    prompt_text, prompt_ids = _token_line(log, "prompt_token_ids", log_path)
    if len(output_ids) != output_tokens or sha256_text(output_text) != output_sha256:
        raise BenchmarkContractError(f"deterministic output token contract failed: {log_path}")
    if len(prompt_ids) != prompt_tokens or sha256_text(prompt_text) != prompt_sha256:
        raise BenchmarkContractError(f"prompt token contract failed: {log_path}")
    if payload.get("token_ids") is not None and payload["token_ids"] != output_ids:
        raise BenchmarkContractError(f"JSON/log output tokens differ: {json_path}")

    loads = list(A4B_LOAD.finditer(log))
    if not loads:
        raise BenchmarkContractError(f"resident CUDA A4B load marker missing: {log_path}")
    layers, source_count, source_mib, workspace_mib, budget_mib = (
        int(value) for value in loads[-1].groups()
    )
    if layers != 30 or source_count <= 0 or source_mib <= 0 or workspace_mib <= 0:
        raise BenchmarkContractError(f"invalid resident CUDA A4B load marker: {log_path}")
    if budget_mib != 16_384:
        raise BenchmarkContractError(f"CUDA A4B budget is {budget_mib}, expected 16384")

    cuda = payload.get("cuda")
    cuda_generate = payload.get("cuda_generate")
    if not isinstance(cuda, dict) or not isinstance(cuda_generate, dict):
        raise BenchmarkContractError(f"CUDA runtime counters missing: {json_path}")
    counters = {
        "a4b_resident_source_bytes": exact_int(
            cuda, "a4b_resident_source_bytes", json_path
        ),
        "a4b_resident_source_count": exact_int(
            cuda, "a4b_resident_source_count", json_path
        ),
        "a4b_route_calls": exact_int(cuda_generate, "a4b_route_calls", json_path),
        "a4b_decode_calls": exact_int(cuda_generate, "a4b_decode_calls", json_path),
        "a4b_prefill_calls": exact_int(cuda_generate, "a4b_prefill_calls", json_path),
        "graph_capture_replays": exact_int(
            cuda_generate, "graph_capture_replays", json_path
        ),
        "graph_capture_persistent_replays": exact_int(
            cuda_generate, "graph_capture_persistent_replays", json_path
        ),
        "a4b_compact_down_hits": exact_int(
            cuda_generate, "a4b_compact_down_hits", json_path
        ),
        "a4b_exact_lm_head_hits": exact_int(
            cuda_generate, "a4b_exact_lm_head_hits", json_path
        ),
        "device_kv_attempts": exact_int(
            cuda_generate, "device_kv_attempts", json_path
        ),
        "device_kv_successes": exact_int(
            cuda_generate, "device_kv_successes", json_path
        ),
    }
    if (
        counters["a4b_resident_source_bytes"] <= 0
        or counters["a4b_resident_source_count"] != source_count
        or counters["a4b_route_calls"] <= 0
        or counters["a4b_decode_calls"] <= 0
        or counters["a4b_prefill_calls"] <= 0
        or counters["graph_capture_replays"] <= 0
        or counters["graph_capture_persistent_replays"] <= 0
        or counters["a4b_compact_down_hits"] <= 0
        or counters["a4b_exact_lm_head_hits"] <= 0
        or counters["device_kv_attempts"] <= 0
        or counters["device_kv_successes"] != counters["device_kv_attempts"]
    ):
        raise BenchmarkContractError(f"CUDA A4B execution counters are incomplete: {json_path}")

    zero_counters = (
        "a4b_compact_down_fallbacks",
        "a4b_exact_lm_head_fallbacks",
        "cross_backend_copies",
        "cross_backend_sync_fallbacks",
        "graph_capture_capacity_skips",
        "graph_capture_discards",
        "graph_capture_update_failures",
        "decoder_runtime_linear_slot_prepare_misses",
        "decoder_runtime_rms_norm_slot_prepare_misses",
        "decoder_runtime_linear_apply_misses",
        "decoder_runtime_rms_norm_apply_misses",
        "lm_head_argmax_fallbacks",
        "device_kv_fail_batch",
        "device_kv_fail_no_cache",
        "device_kv_fail_no_hook",
        "device_kv_fail_no_storage",
        "device_kv_fail_read",
        "device_kv_fail_shape",
        "device_kv_fail_write",
    )
    nonzero_fallbacks: dict[str, int] = {}
    for key in zero_counters:
        value = exact_int(cuda_generate, key, json_path)
        if value != 0:
            nonzero_fallbacks[key] = value
    if nonzero_fallbacks:
        raise BenchmarkContractError(
            f"CUDA A4B fallback/skip counters are nonzero: {nonzero_fallbacks}: {json_path}"
        )

    timing = payload.get("timing_ms")
    if not isinstance(timing, dict):
        raise BenchmarkContractError(f"timing_ms missing: {json_path}")
    decode_ms = float(timing.get("decode_inner") or timing.get("decode") or 0)
    if not math.isfinite(decode_ms) or decode_ms <= 0:
        raise BenchmarkContractError(f"invalid decode timing: {json_path}")
    return {
        "decode_ms": decode_ms,
        "decode_tok_s": max(output_tokens - 1, 1) * 1000.0 / decode_ms,
        "output_token_ids_sha256": output_sha256,
        "counters": counters,
    }


def parse_llama_sample(log_path: Path, *, expected_device: str) -> dict[str, Any]:
    text = log_path.read_text(errors="replace")
    decode_matches = LLAMA_DECODE.findall(text)
    if not decode_matches:
        raise BenchmarkContractError(f"llama.cpp decode throughput missing: {log_path}")
    tok_s = float(decode_matches[-1])
    if not math.isfinite(tok_s) or tok_s <= 0:
        raise BenchmarkContractError(f"invalid llama.cpp throughput: {log_path}")

    legacy_device_counts = {
        int(value) for value in LLAMA_CUDA_DEVICE_COUNT.findall(text)
    }
    inventory = {
        (int(index), name.strip())
        for index, name in LLAMA_CUDA_DEVICE_INVENTORY.findall(text)
    }
    inventory_indices = {index for index, _ in inventory}
    inventory_names = {name for _, name in inventory}
    if legacy_device_counts:
        has_exact_device_count = legacy_device_counts == {1}
    else:
        has_exact_device_count = inventory_indices == {0}
    if not has_exact_device_count:
        raise BenchmarkContractError(
            f"llama.cpp did not report exactly one CUDA device: {log_path}"
        )
    if inventory_names and inventory_names != {expected_device}:
        raise BenchmarkContractError(
            f"llama.cpp CUDA inventory is {sorted(inventory_names)!r}, "
            f"expected {expected_device!r}: {log_path}"
        )
    devices = {value.strip() for value in LLAMA_CUDA_DEVICE.findall(text)}
    if devices != {expected_device}:
        raise BenchmarkContractError(
            f"llama.cpp CUDA device is {sorted(devices)!r}, expected {expected_device!r}: "
            f"{log_path}"
        )
    offloads = LLAMA_GPU_OFFLOAD.findall(text)
    if not offloads:
        raise BenchmarkContractError(
            f"llama.cpp full GPU layer-offload marker is missing: {log_path}"
        )
    offload_pairs = {
        (int(offloaded), int(total)) for offloaded, total in offloads
    }
    if len(offload_pairs) != 1:
        raise BenchmarkContractError(
            f"llama.cpp GPU layer-offload markers disagree: {log_path}"
        )
    offloaded_layers, total_layers = next(iter(offload_pairs))
    if offloaded_layers <= 0 or offloaded_layers != total_layers:
        raise BenchmarkContractError(
            f"llama.cpp GPU layer offload is {offloaded_layers}/{total_layers}, "
            f"expected complete offload: {log_path}"
        )
    return {
        "decode_tok_s": tok_s,
        "cuda_device": expected_device,
        "cuda_device_count": 1,
        "offloaded_layers": offloaded_layers,
        "total_layers": total_layers,
    }


def cuda_identity(expected_device: str) -> dict[str, str]:
    command = (
        "nvidia-smi",
        "--query-gpu=name,compute_cap,uuid,driver_version",
        "--format=csv,noheader,nounits",
    )
    lines = subprocess.check_output(command, text=True).strip().splitlines()
    if len(lines) != 1:
        raise BenchmarkContractError(f"qualification requires exactly one CUDA GPU, found {len(lines)}")
    values = [part.strip() for part in lines[0].split(",")]
    if len(values) != 4:
        raise BenchmarkContractError("unexpected nvidia-smi identity output")
    name, compute_capability, uuid, driver = values
    if name != expected_device or compute_capability != "8.9":
        raise BenchmarkContractError(
            f"device is {name} sm_{compute_capability.replace('.', '')}, "
            f"expected {expected_device} sm_89"
        )
    return {
        "name": name,
        "compute_capability": compute_capability,
        "uuid": uuid,
        "driver_version": driver,
    }


def run_logged(command: list[str], log_path: Path, environment: dict[str, str]) -> None:
    with log_path.open("w") as log:
        completed = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
            text=True,
            check=False,
        )
    if completed.returncode:
        raise BenchmarkContractError(
            f"command exited {completed.returncode}; see {log_path}"
        )


def antfly_command(
    binary: Path,
    model: Path,
    prompt: str,
    output_tokens: int,
    json_path: Path,
) -> list[str]:
    command = [str(binary)]
    if binary.name == "antfly":
        command.append("inference")
    command.extend(
        (
            "generate",
            str(model),
            prompt,
            "--backend",
            "cuda",
            "--a4b-residency-mode",
            "resident",
            "--a4b-memory-budget-mb",
            "16384",
            "--backend-budget-mb",
            "16384",
            "--combined-budget-mb",
            "24576",
            "--cache-dtype",
            "f16",
            "--max-tokens",
            str(output_tokens),
            "--temperature",
            "0",
            "--raw-prompt",
            "--ignore-eos",
            "--print-token-count",
            "--print-finish-reason",
            "--print-token-ids",
            "--print-prompt-token-ids",
            "--print-timing",
            "--json-timing",
            str(json_path),
        )
    )
    return command


def llama_command(binary: Path, model: Path, prompt: str, output_tokens: int) -> list[str]:
    return [
        str(binary),
        "-m",
        str(model),
        "-p",
        prompt,
        "-n",
        str(output_tokens),
        "-ngl",
        "999",
        "--temp",
        "0",
        "--ignore-eos",
        "--no-display-prompt",
        "--single-turn",
    ]


def run(args: argparse.Namespace) -> dict[str, Any]:
    model = args.model.resolve()
    antfly = args.antfly_bin.resolve()
    llama = args.llama_bin.resolve()
    out_dir = args.out_dir.resolve()
    for path, label in ((model, "model"), (antfly, "Antfly binary"), (llama, "llama binary")):
        if not path.is_file():
            raise BenchmarkContractError(f"{label} does not exist: {path}")
    if model.stat().st_size != QUALIFIED_MODEL_BYTES:
        raise BenchmarkContractError(
            f"model size is {model.stat().st_size}, expected {QUALIFIED_MODEL_BYTES}"
        )
    model_sha = sha256_file(model)
    if model_sha != QUALIFIED_MODEL_SHA256:
        raise BenchmarkContractError(f"model SHA-256 is not the qualified artifact: {model_sha}")
    if args.runs < 3 or args.warmups < 1:
        raise BenchmarkContractError("qualification requires at least 1 warmup and 3 measured runs")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise BenchmarkContractError(f"--out-dir must be empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    gpu = cuda_identity(args.expected_device)
    repo = SCRIPT_DIR.parents[3]
    revision = subprocess.check_output(("git", "-C", str(repo), "rev-parse", "HEAD"), text=True).strip()
    tracked_status = subprocess.check_output(
        ("git", "-C", str(repo), "status", "--porcelain=v1", "--untracked-files=no")
    ).strip()
    if tracked_status:
        raise BenchmarkContractError("qualification requires a clean tracked source checkout")
    llama_sha = require_sha256(llama, args.expected_llama_sha256, "llama.cpp binary")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("TERMITE_", "ANTFLY_INFERENCE_"))
    }
    environment["ANTFLY_INFERENCE_JSON_TOKEN_IDS"] = "1"
    environment["ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"] = "required"
    # Current llama.cpp emits its device inventory and complete-offload
    # markers at trace verbosity. Use the environment form so that evidence
    # is enabled from process startup.
    environment["LLAMA_ARG_LOG_VERBOSITY"] = "4"
    replay_kv_capacity = cuda_replay_kv_capacity(
        args.expected_prompt_tokens, args.output_tokens
    )
    environment["ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"] = str(
        replay_kv_capacity
    )
    samples: list[dict[str, Any]] = []
    total_pairs = args.warmups + args.runs
    for pair in range(total_pairs):
        warmup = pair < args.warmups
        for runtime in (("antfly", "llama") if pair % 2 == 0 else ("llama", "antfly")):
            label = f"{pair:02d}-{'warmup' if warmup else 'run'}-{runtime}"
            log_path = out_dir / f"{label}.log"
            if runtime == "antfly":
                json_path = out_dir / f"{label}.json"
                run_logged(
                    antfly_command(antfly, model, args.prompt, args.output_tokens, json_path),
                    log_path,
                    environment,
                )
                parsed = parse_antfly_sample(
                    json_path,
                    log_path,
                    prompt_tokens=args.expected_prompt_tokens,
                    prompt_sha256=args.expected_prompt_sha256,
                    output_tokens=args.output_tokens,
                    output_sha256=args.expected_output_sha256,
                )
            else:
                run_logged(
                    llama_command(llama, model, args.prompt, args.output_tokens),
                    log_path,
                    environment,
                )
                parsed = parse_llama_sample(
                    log_path, expected_device=args.expected_device
                )
            samples.append({"pair": pair, "warmup": warmup, "runtime": runtime, **parsed})

    measured = [sample for sample in samples if not sample["warmup"]]
    antfly_rates = [sample["decode_tok_s"] for sample in measured if sample["runtime"] == "antfly"]
    llama_rates = [sample["decode_tok_s"] for sample in measured if sample["runtime"] == "llama"]
    antfly_stats = metric_stats(antfly_rates)
    llama_stats = metric_stats(llama_rates)
    ratio = antfly_stats["median"] / llama_stats["median"]
    failures: list[str] = []
    if antfly_stats["cv"] > args.max_cv:
        failures.append(f"Antfly decode CV {antfly_stats['cv']:.4f} exceeds {args.max_cv:.4f}")
    if llama_stats["cv"] > args.max_cv:
        failures.append(f"llama.cpp decode CV {llama_stats['cv']:.4f} exceeds {args.max_cv:.4f}")
    if ratio < args.min_llama_ratio:
        failures.append(
            f"Antfly/llama.cpp decode ratio {ratio:.4f} is below {args.min_llama_ratio:.4f}"
        )
    receipt = {
        "schema": SCHEMA,
        "passed": not failures,
        "failures": failures,
        "git_revision": revision,
        "git_dirty": False,
        "runner_sha256": sha256_file(Path(__file__).resolve()),
        "model": {
            "path": str(model),
            "bytes": model.stat().st_size,
            "sha256": model_sha,
        },
        "executables": {
            "antfly": {"path": str(antfly), "sha256": sha256_file(antfly)},
            "llama": {"path": str(llama), "sha256": llama_sha},
        },
        "gpu": gpu,
        "environment": {
            "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY": str(
                replay_kv_capacity
            ),
            "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "required",
            "ANTFLY_INFERENCE_JSON_TOKEN_IDS": "1",
        },
        "samples": samples,
        "antfly_decode": antfly_stats,
        "llama_decode": llama_stats,
        "antfly_to_llama_ratio": ratio,
        "minimum_ratio": args.min_llama_ratio,
    }
    (out_dir / "qualification.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    return receipt


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--model", type=Path, required=True)
    result.add_argument("--antfly-bin", type=Path, required=True)
    result.add_argument("--llama-bin", type=Path, required=True)
    result.add_argument("--expected-llama-sha256", required=True)
    result.add_argument("--out-dir", type=Path, required=True)
    result.add_argument("--prompt", required=True)
    result.add_argument("--expected-prompt-tokens", type=int, required=True)
    result.add_argument("--expected-prompt-sha256", required=True)
    result.add_argument("--expected-output-sha256", default=QUALIFIED_OUTPUT_SHA256)
    result.add_argument("--output-tokens", type=int, default=128)
    result.add_argument("--expected-device", default="NVIDIA L4")
    result.add_argument("--warmups", type=int, default=1)
    result.add_argument("--runs", type=int, default=3)
    result.add_argument("--min-llama-ratio", type=float, default=0.8)
    result.add_argument("--max-cv", type=float, default=0.12)
    return result


def main() -> int:
    try:
        receipt = run(parser().parse_args())
    except (BenchmarkContractError, OSError, subprocess.SubprocessError) as exc:
        print(f"CUDA A4B qualification failed: {exc}", file=sys.stderr)
        return 2
    print(
        f"CUDA A4B ratio={receipt['antfly_to_llama_ratio']:.4f} "
        f"passed={receipt['passed']}"
    )
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
