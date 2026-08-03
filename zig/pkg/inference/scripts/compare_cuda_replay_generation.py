#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare CUDA persistent graph replay against recapture/update generation.

This is intentionally a generated-output check, not a first-token logit check:
the persistent final-hidden replay path only runs on decode steps after prefill.
For each model/prompt pair the script runs the existing `generate` command twice:
one persistent replay run and one recapture/update control run.  It fails if the
token ids differ or if the graph counters do not show the expected mode.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path


TOKEN_IDS_RE = re.compile(r"^token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[4]


def inference_root_from_script() -> Path:
    return Path(__file__).resolve().parents[1]


def load_prompts(path: Path, max_prompts: int) -> list[str]:
    prompts: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        prompts.append(line)
        if max_prompts and len(prompts) >= max_prompts:
            break
    return prompts


def slug(value: str) -> str:
    out = []
    for ch in value.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "-":
            out.append("-")
    text = "".join(out).strip("-")
    return text[:80] or "item"


def env_for_run(
    persistent: bool,
    capture_greedy_token: bool,
    capture_min_alloc_seq: int | None,
    force_kv_capacity: int | None,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "ANTFLY_INFERENCE_CUDA_CAPTURE_FINAL_HIDDEN": "1",
            "ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC": "1",
            "ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS": "1",
            "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN": "1",
            "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD": "0",
            "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP": "0",
        }
    )
    if persistent:
        env["ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY"] = "1"
    else:
        env.pop("ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY", None)
    if persistent and force_kv_capacity is not None:
        env["ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"] = str(force_kv_capacity)
    else:
        env.pop("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY", None)
    if capture_greedy_token:
        env["ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN"] = "1"
    else:
        env.pop("ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN", None)
    if capture_min_alloc_seq is not None:
        env["ANTFLY_INFERENCE_CUDA_CAPTURE_MIN_ALLOC_SEQ"] = str(capture_min_alloc_seq)
    else:
        env.pop("ANTFLY_INFERENCE_CUDA_CAPTURE_MIN_ALLOC_SEQ", None)
    return env


def run_generate(
    *,
    binary: Path,
    cwd: Path,
    model: str,
    prompt: str,
    timing_path: Path,
    max_tokens: int,
    timeout_sec: int,
    backend_budget_mb: int,
    combined_budget_mb: int,
    kv_budget_mb: int,
    scratch_budget_mb: int,
    raw_prompt: bool,
    no_chat_template: bool,
    persistent: bool,
    capture_greedy_token: bool,
    capture_min_alloc_seq: int | None,
    force_kv_capacity: int | None,
) -> dict[str, object]:
    cmd = [
        str(binary),
        "generate",
        model,
        prompt,
        "--backend",
        "cuda",
        "--combined-budget-mb",
        str(combined_budget_mb),
        "--backend-budget-mb",
        str(backend_budget_mb),
        "--kv-budget-mb",
        str(kv_budget_mb),
        "--scratch-budget-mb",
        str(scratch_budget_mb),
        "--max-tokens",
        str(max_tokens),
        "--temperature",
        "0",
        "--print-token-count",
        "--print-token-ids",
        "--print-timing",
        "--json-timing",
        str(timing_path),
    ]
    if raw_prompt:
        cmd.append("--raw-prompt")
    if no_chat_template:
        cmd.append("--no-chat-template")

    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env_for_run(persistent, capture_greedy_token, capture_min_alloc_seq, force_kv_capacity),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_sec,
            check=False,
        )
        exit_code = proc.returncode
        output = proc.stdout
    except subprocess.TimeoutExpired as err:
        exit_code = 124
        output_raw = err.stdout or ""
        output = output_raw if isinstance(output_raw, str) else output_raw.decode("utf-8", errors="replace")
        output += f"\ncommand timed out after {timeout_sec}s\n"
    elapsed = time.time() - started
    token_match = TOKEN_IDS_RE.search(output)
    token_ids = [int(part) for part in token_match.group("ids").split()] if token_match else []
    timing = json.loads(timing_path.read_text(encoding="utf-8")) if timing_path.is_file() else {}
    return {
        "cmd": cmd,
        "exit_code": exit_code,
        "elapsed_sec": elapsed,
        "output": output,
        "token_ids": token_ids,
        "timing": timing,
    }


def cuda_counter(result: dict[str, object], key: str) -> int:
    timing = result.get("timing")
    if not isinstance(timing, dict):
        return 0
    cuda = timing.get("cuda")
    if not isinstance(cuda, dict):
        return 0
    value = cuda.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def decode_rate(result: dict[str, object]) -> float:
    timing = result.get("timing")
    if not isinstance(timing, dict):
        return 0.0
    value = timing.get("decode_tok_per_s", 0.0)
    return float(value) if isinstance(value, (int, float)) else 0.0


def validate_transfer_and_memory(
    *,
    label: str,
    result: dict[str, object],
    token_count: int,
    max_extra_scalar_downloads: int,
    max_temp_evictions: int,
) -> list[str]:
    errors: list[str] = []
    max_scalar_d2h = 4 * (token_count + max_extra_scalar_downloads)
    d2h_bytes = cuda_counter(result, "d2h_bytes")
    temp_evictions = cuda_counter(result, "temp_buffer_evictions")
    lm_head_fallbacks = cuda_counter(result, "lm_head_argmax_fallbacks")
    if d2h_bytes > max_scalar_d2h:
        errors.append(
            f"{label} d2h_bytes={d2h_bytes} exceeds scalar-only budget {max_scalar_d2h}"
        )
    if temp_evictions > max_temp_evictions:
        errors.append(
            f"{label} temp_buffer_evictions={temp_evictions} exceeds budget {max_temp_evictions}"
        )
    if lm_head_fallbacks != 0:
        errors.append(f"{label} lm_head_argmax_fallbacks={lm_head_fallbacks}")
    return errors


def validate_pair(
    model: str,
    prompt: str,
    persistent: dict[str, object],
    recapture: dict[str, object],
    max_extra_scalar_downloads: int,
    max_temp_evictions: int,
    min_persistent_replays: int,
    min_capacity_skips: int,
    max_capacity_skips: int | None,
) -> list[str]:
    errors: list[str] = []
    if persistent["exit_code"] != 0:
        errors.append(f"persistent run failed exit={persistent['exit_code']}")
    if recapture["exit_code"] != 0:
        errors.append(f"recapture run failed exit={recapture['exit_code']}")
    if not persistent["token_ids"]:
        errors.append("persistent token_ids missing")
    if not recapture["token_ids"]:
        errors.append("recapture token_ids missing")
    if persistent["token_ids"] != recapture["token_ids"]:
        errors.append(
            "token_ids mismatch: "
            f"persistent={persistent['token_ids']} recapture={recapture['token_ids']}"
        )

    persistent_replays = cuda_counter(persistent, "graph_capture_persistent_replays")
    recapture_persistent_replays = cuda_counter(recapture, "graph_capture_persistent_replays")
    persistent_skips = cuda_counter(persistent, "graph_capture_capacity_skips")
    recapture_skips = cuda_counter(recapture, "graph_capture_capacity_skips")
    if min_persistent_replays > 0 and persistent_replays <= 0:
        errors.append("persistent run did not report graph_capture_persistent_replays > 0")
    if persistent_replays < min_persistent_replays:
        errors.append(
            f"persistent run reported persistent_replays={persistent_replays}, below minimum {min_persistent_replays}"
        )
    if recapture_persistent_replays != 0:
        errors.append(f"recapture run unexpectedly reported persistent_replays={recapture_persistent_replays}")
    if persistent_skips < min_capacity_skips:
        errors.append(
            f"persistent run reported capacity_skips={persistent_skips}, below minimum {min_capacity_skips}"
        )
    if max_capacity_skips is not None and persistent_skips > max_capacity_skips:
        errors.append(
            f"persistent run reported capacity_skips={persistent_skips}, above maximum {max_capacity_skips}"
        )
    if min_capacity_skips == 0 and max_capacity_skips is None and persistent_skips != 0:
        errors.append(f"persistent run reported capacity_skips={persistent_skips}")
    if recapture_skips != 0:
        errors.append(f"recapture run reported capacity_skips={recapture_skips}")

    require_launch_savings = min_capacity_skips == 0
    if require_launch_savings and cuda_counter(persistent, "kernel_launches") > cuda_counter(recapture, "kernel_launches"):
        errors.append("persistent run used more kernel launches than recapture control")

    token_count = len(persistent["token_ids"]) if persistent["token_ids"] else len(recapture["token_ids"])
    errors.extend(
        validate_transfer_and_memory(
            label="persistent",
            result=persistent,
            token_count=token_count,
            max_extra_scalar_downloads=max_extra_scalar_downloads,
            max_temp_evictions=max_temp_evictions,
        )
    )
    errors.extend(
        validate_transfer_and_memory(
            label="recapture",
            result=recapture,
            token_count=token_count,
            max_extra_scalar_downloads=max_extra_scalar_downloads,
            max_temp_evictions=max_temp_evictions,
        )
    )

    if "q4" in model.lower() and cuda_counter(persistent, "q4k_decode_fast_fallbacks") != 0:
        errors.append("persistent Q4 run reported q4k_decode_fast_fallbacks != 0")
    if "q4" in model.lower() and cuda_counter(recapture, "q4k_decode_fast_fallbacks") != 0:
        errors.append("recapture Q4 run reported q4k_decode_fast_fallbacks != 0")

    if errors:
        header = f"model={model} prompt={prompt!r}"
        return [header, *[f"  - {err}" for err in errors]]
    return []


def write_run_artifacts(path: Path, result: dict[str, object]) -> None:
    path.with_suffix(".log").write_text(str(result["output"]), encoding="utf-8")
    slim = dict(result)
    slim["output"] = f"<see {path.with_suffix('.log').name}>"
    path.write_text(json.dumps(slim, indent=2), encoding="utf-8")


def main() -> int:
    repo_root = repo_root_from_script()
    inference_root = inference_root_from_script()
    default_prompt_file = inference_root / "testdata/gemma4_replay_prompts.txt"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=repo_root / "zig/pkg/inference/zig-out/bin/antfly-inference")
    parser.add_argument("--model", action="append", required=True, help="Model directory. Repeat for Q8/Q4.")
    parser.add_argument("--prompt", action="append", default=[], help="Prompt to test. Can repeat.")
    parser.add_argument("--prompt-file", type=Path, default=None)
    parser.add_argument("--max-prompts", type=int, default=0)
    parser.add_argument("--max-tokens", type=int, default=12)
    parser.add_argument("--timeout-sec", type=int, default=360)
    parser.add_argument("--out-dir", type=Path, default=Path("/tmp/gemma-cuda-replay-compare"))
    parser.add_argument("--combined-budget-mb", type=int, default=22000)
    parser.add_argument("--backend-budget-mb", type=int, default=19000)
    parser.add_argument("--kv-budget-mb", type=int, default=512)
    parser.add_argument("--scratch-budget-mb", type=int, default=1024)
    parser.add_argument(
        "--max-extra-scalar-downloads",
        type=int,
        default=2,
        help="Allow this many extra 4-byte token-id downloads beyond emitted tokens.",
    )
    parser.add_argument(
        "--max-temp-evictions",
        type=int,
        default=0,
        help="Maximum allowed CUDA temp-buffer evictions per generate run.",
    )
    parser.add_argument(
        "--min-persistent-replays",
        type=int,
        default=None,
        help="Minimum required persistent replay count for each persistent run.",
    )
    parser.add_argument(
        "--force-kv-capacity",
        type=int,
        default=None,
        help=(
            "Clamp the persistent run's captured KV replay capacity. "
            "Useful for forcing over-capacity fallback validation."
        ),
    )
    parser.add_argument(
        "--min-capacity-skips",
        type=int,
        default=0,
        help=(
            "Minimum required graph_capture_capacity_skips for each persistent run. "
            "Defaults to 1 when --force-kv-capacity is set, otherwise 0."
        ),
    )
    parser.add_argument(
        "--max-capacity-skips",
        type=int,
        default=None,
        help="Optional maximum allowed graph_capture_capacity_skips for each persistent run.",
    )
    parser.add_argument("--raw-prompt", action="store_true")
    parser.add_argument("--no-chat-template", action="store_true")
    parser.add_argument(
        "--capture-greedy-token",
        action="store_true",
        help="Also capture the device greedy-token selection as the graph boundary.",
    )
    parser.add_argument(
        "--capture-min-alloc-seq",
        type=int,
        default=None,
        help="Override ANTFLY_INFERENCE_CUDA_CAPTURE_MIN_ALLOC_SEQ for replay probes.",
    )
    args = parser.parse_args()
    if args.force_kv_capacity is not None and args.force_kv_capacity <= 0:
        raise SystemExit("--force-kv-capacity must be positive")
    if args.min_capacity_skips < 0:
        raise SystemExit("--min-capacity-skips must be non-negative")
    if args.max_capacity_skips is not None and args.max_capacity_skips < 0:
        raise SystemExit("--max-capacity-skips must be non-negative")
    if args.max_capacity_skips is not None and args.max_capacity_skips < args.min_capacity_skips:
        raise SystemExit("--max-capacity-skips must be >= --min-capacity-skips")
    if args.force_kv_capacity is not None and args.min_capacity_skips == 0:
        args.min_capacity_skips = 1
    if args.min_persistent_replays is None:
        args.min_persistent_replays = 0 if args.force_kv_capacity is not None else 1
    if args.min_persistent_replays < 0:
        raise SystemExit("--min-persistent-replays must be non-negative")

    prompts = list(args.prompt)
    prompt_file = args.prompt_file
    if prompt_file is None and not prompts:
        prompt_file = default_prompt_file
    if prompt_file:
        prompts.extend(load_prompts(prompt_file, args.max_prompts))
    if args.max_prompts and len(prompts) > args.max_prompts:
        prompts = prompts[: args.max_prompts]
    if not prompts:
        raise SystemExit("no prompts supplied")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    summary: list[dict[str, object]] = []
    for model in args.model:
        model_slug = slug(Path(model).name)
        for prompt_index, prompt in enumerate(prompts):
            prompt_slug = f"{prompt_index:02d}-{slug(prompt)}"
            base = args.out_dir / f"{model_slug}-{prompt_slug}"
            persistent_timing = base.with_name(base.name + "-persistent-timing.json")
            recapture_timing = base.with_name(base.name + "-recapture-timing.json")

            persistent = run_generate(
                binary=args.binary,
                cwd=repo_root,
                model=model,
                prompt=prompt,
                timing_path=persistent_timing,
                max_tokens=args.max_tokens,
                timeout_sec=args.timeout_sec,
                backend_budget_mb=args.backend_budget_mb,
                combined_budget_mb=args.combined_budget_mb,
                kv_budget_mb=args.kv_budget_mb,
                scratch_budget_mb=args.scratch_budget_mb,
                raw_prompt=args.raw_prompt,
                no_chat_template=args.no_chat_template,
                persistent=True,
                capture_greedy_token=args.capture_greedy_token,
                capture_min_alloc_seq=args.capture_min_alloc_seq,
                force_kv_capacity=args.force_kv_capacity,
            )
            recapture = run_generate(
                binary=args.binary,
                cwd=repo_root,
                model=model,
                prompt=prompt,
                timing_path=recapture_timing,
                max_tokens=args.max_tokens,
                timeout_sec=args.timeout_sec,
                backend_budget_mb=args.backend_budget_mb,
                combined_budget_mb=args.combined_budget_mb,
                kv_budget_mb=args.kv_budget_mb,
                scratch_budget_mb=args.scratch_budget_mb,
                raw_prompt=args.raw_prompt,
                no_chat_template=args.no_chat_template,
                persistent=False,
                capture_greedy_token=args.capture_greedy_token,
                capture_min_alloc_seq=args.capture_min_alloc_seq,
                force_kv_capacity=args.force_kv_capacity,
            )
            write_run_artifacts(base.with_name(base.name + "-persistent.json"), persistent)
            write_run_artifacts(base.with_name(base.name + "-recapture.json"), recapture)
            pair_failures = validate_pair(
                model,
                prompt,
                persistent,
                recapture,
                args.max_extra_scalar_downloads,
                args.max_temp_evictions,
                args.min_persistent_replays,
                args.min_capacity_skips,
                args.max_capacity_skips,
            )
            failures.extend(pair_failures)

            row = {
                "model": model,
                "prompt": prompt,
                "tokens": len(persistent["token_ids"]),
                "token_ids": persistent["token_ids"],
                "persistent": {
                    "decode_tok_per_s": decode_rate(persistent),
                    "kernel_launches": cuda_counter(persistent, "kernel_launches"),
                    "begins": cuda_counter(persistent, "graph_capture_begins"),
                    "replays": cuda_counter(persistent, "graph_capture_replays"),
                    "persistent_replays": cuda_counter(persistent, "graph_capture_persistent_replays"),
                    "capacity_skips": cuda_counter(persistent, "graph_capture_capacity_skips"),
                    "d2h_bytes": cuda_counter(persistent, "d2h_bytes"),
                    "temp_buffer_evictions": cuda_counter(persistent, "temp_buffer_evictions"),
                    "temp_buffer_cached_bytes": cuda_counter(persistent, "temp_buffer_cached_bytes"),
                    "lm_head_argmax_fallbacks": cuda_counter(persistent, "lm_head_argmax_fallbacks"),
                    "lm_head_argmax_fused_q8": cuda_counter(persistent, "lm_head_argmax_fused_q8"),
                    "lm_head_argmax_fused_q4": cuda_counter(persistent, "lm_head_argmax_fused_q4"),
                },
                "recapture": {
                    "decode_tok_per_s": decode_rate(recapture),
                    "kernel_launches": cuda_counter(recapture, "kernel_launches"),
                    "begins": cuda_counter(recapture, "graph_capture_begins"),
                    "replays": cuda_counter(recapture, "graph_capture_replays"),
                    "update_successes": cuda_counter(recapture, "graph_capture_update_successes"),
                    "persistent_replays": cuda_counter(recapture, "graph_capture_persistent_replays"),
                    "capacity_skips": cuda_counter(recapture, "graph_capture_capacity_skips"),
                    "d2h_bytes": cuda_counter(recapture, "d2h_bytes"),
                    "temp_buffer_evictions": cuda_counter(recapture, "temp_buffer_evictions"),
                    "temp_buffer_cached_bytes": cuda_counter(recapture, "temp_buffer_cached_bytes"),
                    "lm_head_argmax_fallbacks": cuda_counter(recapture, "lm_head_argmax_fallbacks"),
                    "lm_head_argmax_fused_q8": cuda_counter(recapture, "lm_head_argmax_fused_q8"),
                    "lm_head_argmax_fused_q4": cuda_counter(recapture, "lm_head_argmax_fused_q4"),
                },
                "capture_greedy_token": args.capture_greedy_token,
                "capture_min_alloc_seq": args.capture_min_alloc_seq,
                "force_kv_capacity": args.force_kv_capacity,
                "min_capacity_skips": args.min_capacity_skips,
                "max_capacity_skips": args.max_capacity_skips,
            }
            summary.append(row)
            print(
                "PASS" if not pair_failures else "FAIL",
                model,
                f"prompt={prompt_index}",
                f"tokens={row['tokens']}",
                f"persistent_replays={row['persistent']['persistent_replays']}",
                f"capacity_skips={row['persistent']['capacity_skips']}/{row['recapture']['capacity_skips']}",
                f"launches={row['persistent']['kernel_launches']}/{row['recapture']['kernel_launches']}",
                f"d2h={row['persistent']['d2h_bytes']}/{row['recapture']['d2h_bytes']}",
                f"temp_evictions={row['persistent']['temp_buffer_evictions']}/{row['recapture']['temp_buffer_evictions']}",
            )

    summary_path = args.out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if failures:
        print("replay comparison failed:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        print(f"artifacts: {args.out_dir}", file=sys.stderr)
        return 1
    print(f"summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
