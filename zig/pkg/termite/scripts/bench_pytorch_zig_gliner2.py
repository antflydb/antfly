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

"""Narrow GLiNER2 fp32 benchmark harness.

This intentionally starts smaller than the production-readiness matrix:
same text/labels, batch sizes 1 and 8, PyTorch MPS/CPU plus Zig native/Metal.
Metal runs go through the repository debug wrapper by default so API
validation stays on while we are chasing kernel regressions.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


DEFAULT_TEXT = (
    "John Smith works for Apple Inc. and lives in San Francisco. "
    "Apple Inc. is located in Cupertino."
)
DEFAULT_LABELS = ("person", "organization", "location")


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[4]


def parse_args() -> argparse.Namespace:
    root = repo_root_from_script()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", default="/private/tmp/gliner2-fp32-export")
    parser.add_argument(
        "--pytorch-model-dir",
        default=str(
            Path.home()
            / ".cache/huggingface/hub/models--fastino--gliner2-base-v1/snapshots/f5b2ecedebe4381b088c1cf276f5bf72a52cac54"
        ),
    )
    parser.add_argument("--text", default=DEFAULT_TEXT)
    parser.add_argument("--label", action="append", dest="labels", default=None)
    parser.add_argument("--batch-size", type=int, action="append", dest="batch_sizes", default=None)
    parser.add_argument("--warmup-iters", type=int, default=1)
    parser.add_argument("--measure-iters", type=int, default=3)
    parser.add_argument("--out-dir", default="/private/tmp/termite-gliner2-fp32-bench")
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--repo-root", default=str(root))
    parser.add_argument("--skip-pytorch", action="store_true")
    parser.add_argument("--skip-zig", action="store_true")
    parser.add_argument("--skip-native", action="store_true")
    parser.add_argument("--skip-metal", action="store_true")
    parser.add_argument("--skip-full-graph", action="store_true")
    parser.add_argument("--profile", action="store_true", help="Enable verbose GLiNER profile output in Zig runs.")
    parser.add_argument("--pytorch-device", choices=("mps", "cpu"), action="append", default=None)
    parser.add_argument("--zig-cache-dir", default="/private/tmp/termite-zig-cache-gliner2-bench/local")
    parser.add_argument("--zig-global-cache-dir", default="/private/tmp/termite-zig-cache-gliner2-bench/global")
    parser.add_argument("--metal-debug", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--metal-timeout", type=int, default=180)
    parser.add_argument("--extra-zig-arg", action="append", default=[])
    return parser.parse_args()


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, max(0, int((len(sorted_values) * pct + 99) // 100 - 1)))
    return sorted_values[idx]


def summarize_ms(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "avg_ms": statistics.fmean(values) if values else 0.0,
        "p50_ms": ordered[len(ordered) // 2] if ordered else 0.0,
        "p95_ms": percentile(ordered, 95.0),
        "min_ms": ordered[0] if ordered else 0.0,
        "max_ms": ordered[-1] if ordered else 0.0,
    }


def count_entities(result: Any) -> tuple[int, float]:
    entities = result.get("entities", {}) if isinstance(result, dict) else {}
    count = 0
    score_sum = 0.0
    for rows in entities.values():
        if not isinstance(rows, list):
            continue
        count += len(rows)
        for row in rows:
            if isinstance(row, dict):
                score_sum += float(row.get("confidence", row.get("score", 0.0)) or 0.0)
    return count, score_sum


def run_pytorch(
    model_dir: str,
    text: str,
    labels: list[str],
    batch_size: int,
    device: str,
    warmup_iters: int,
    measure_iters: int,
) -> dict[str, Any]:
    import torch
    from gliner2 import GLiNER2

    available = device != "mps" or torch.backends.mps.is_available()
    if not available:
        return {
            "runner": f"pytorch_{device}",
            "backend": device,
            "batch_size": batch_size,
            "status": "skipped",
            "error": "torch.backends.mps.is_available() is false",
        }

    model = GLiNER2.from_pretrained(model_dir)
    model.to(device)
    model.eval()
    texts = [text] * batch_size

    for _ in range(warmup_iters):
        with torch.inference_mode():
            _ = model.batch_extract_entities(
                texts,
                labels,
                batch_size=batch_size,
                include_confidence=True,
                include_spans=True,
            )
        if device == "mps":
            torch.mps.synchronize()

    samples: list[float] = []
    last_count = 0
    last_score_sum = 0.0
    for _ in range(measure_iters):
        if device == "mps":
            torch.mps.synchronize()
        start = time.perf_counter_ns()
        with torch.inference_mode():
            result = model.batch_extract_entities(
                texts,
                labels,
                batch_size=batch_size,
                include_confidence=True,
                include_spans=True,
            )
        if device == "mps":
            torch.mps.synchronize()
        samples.append((time.perf_counter_ns() - start) / 1.0e6)
        counts = [count_entities(row) for row in result]
        last_count = sum(c for c, _ in counts)
        last_score_sum = sum(s for _, s in counts)

    return {
        "runner": f"pytorch_{device}",
        "backend": device,
        "batch_size": batch_size,
        "mode": "warm_loaded_session",
        "status": "ok",
        "entity_count": last_count,
        "score_sum": last_score_sum,
        "samples_ms": samples,
        **summarize_ms(samples),
    }


def run_command(cmd: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def parse_zig_csv(output: str) -> tuple[dict[str, str] | None, dict[str, str]]:
    lines = [line for line in output.splitlines() if line.startswith("task,") or line.startswith("entities,")]
    if len(lines) < 2:
        raise ValueError("no GLiNER2 CSV rows found")
    reader = csv.DictReader(lines)
    rows = list(reader)
    first_rows = [row for row in rows if row.get("mode") == "first_run"]
    warm_rows = [row for row in rows if row.get("mode") == "warm_loaded_session"]
    if not warm_rows:
        raise ValueError("no warm_loaded_session CSV row found")
    return (first_rows[-1] if first_rows else None), warm_rows[-1]


def zig_result_from_csv(row: dict[str, str], runner: str, graph_runtime: str | None) -> dict[str, Any]:
    def f(name: str) -> float:
        return float(row.get(name, "0") or 0)

    def i(name: str) -> int:
        return int(row.get(name, "0") or 0)

    return {
        "runner": runner,
        "backend": row.get("backend", ""),
        "graph_runtime": graph_runtime or "",
        "batch_size": i("batch_size"),
        "mode": row.get("mode", ""),
        "status": "ok",
        "entity_count": i("entity_count"),
        "score_sum": f("score_sum"),
        "avg_ms": f("avg_ms"),
        "p50_ms": f("p50_ms"),
        "p95_ms": f("p95_ms"),
        "min_ms": f("min_ms"),
        "max_ms": f("max_ms"),
    }


def run_zig(
    args: argparse.Namespace,
    labels: list[str],
    batch_size: int,
    backend: str,
    graph_runtime: str | None,
    out_dir: Path,
) -> dict[str, Any]:
    repo_root = Path(args.repo_root).resolve()
    termite_dir = repo_root / "zig/pkg/termite"
    cmd = [
        args.zig,
        "build",
        "--cache-dir",
        args.zig_cache_dir,
        "--global-cache-dir",
        args.zig_global_cache_dir,
        "-Dmlx=false",
        "-Donnx=false",
        "-Dmetal=true",
        "bench-gliner2-e2e",
        "--",
        "--model-dir",
        args.model_dir,
        "--backend",
        backend,
        "--batch-size",
        str(batch_size),
        "--warmup-iters",
        str(args.warmup_iters),
        "--measure-iters",
        str(args.measure_iters),
        "--format",
        "csv",
        "--text",
        args.text,
    ]
    if graph_runtime:
        cmd.extend(["--graph-runtime", graph_runtime])
    for label in labels:
        cmd.extend(["--label", label])
    cmd.extend(args.extra_zig_arg)

    env = os.environ.copy()
    env.setdefault("TOKENIZERS_PARALLELISM", "false")
    if args.profile:
        env.setdefault("TERMITE_GLINER_PROFILE", "1")
    env.setdefault("TERMITE_METAL_SYNC_MARKERS", "1")

    runner = f"zig_{backend}" + (f"_{graph_runtime}" if graph_runtime else "_eager")
    actual_cmd = cmd
    cwd = termite_dir
    debug_out: Path | None = None
    if backend == "metal" and args.metal_debug:
        debug_out = out_dir / f"debug-{runner}-batch{batch_size}"
        actual_cmd = [
            "bash",
            str(termite_dir / "scripts/debug_metal_command.sh"),
            "command",
            "--api-validate",
            "--timeout",
            str(args.metal_timeout),
            "--cwd",
            str(termite_dir),
            "--out-dir",
            str(debug_out),
            "--",
            *cmd,
        ]
        cwd = repo_root

    completed = run_command(actual_cmd, cwd, env)
    combined = completed.stdout + completed.stderr
    if debug_out is not None:
        debug_stdout = debug_out / "stdout.txt"
        if debug_stdout.exists():
            combined = debug_stdout.read_text(errors="replace") + "\n" + combined
    log_path = out_dir / f"{runner}-batch{batch_size}.log"
    log_path.write_text(combined)
    if completed.returncode != 0:
        return {
            "runner": runner,
            "backend": backend,
            "graph_runtime": graph_runtime or "",
            "batch_size": batch_size,
            "status": "failed",
            "returncode": completed.returncode,
            "log": str(log_path),
            "error": tail_for_error(combined),
        }
    try:
        first_row, warm_row = parse_zig_csv(combined)
        result = zig_result_from_csv(warm_row, runner, graph_runtime)
    except Exception as exc:
        return {
            "runner": runner,
            "backend": backend,
            "graph_runtime": graph_runtime or "",
            "batch_size": batch_size,
            "status": "failed",
            "returncode": completed.returncode,
            "log": str(log_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
    if first_row is not None:
        first_entities = int(first_row.get("entity_count", "0") or 0)
        warm_entities = int(warm_row.get("entity_count", "0") or 0)
        if first_entities != warm_entities:
            result["status"] = "failed"
            result["error"] = f"warm entity_count {warm_entities} != first_run entity_count {first_entities}"
    result["log"] = str(log_path)
    return result


def tail_for_error(output: str, max_lines: int = 40) -> str:
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    lines = [ansi.sub("", line) for line in output.splitlines()]
    return "\n".join(lines[-max_lines:])


def write_outputs(out_dir: Path, rows: list[dict[str, Any]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "results.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    fieldnames = [
        "runner",
        "backend",
        "graph_runtime",
        "batch_size",
        "mode",
        "status",
        "avg_ms",
        "p50_ms",
        "p95_ms",
        "min_ms",
        "max_ms",
        "entity_count",
        "score_sum",
        "error",
        "log",
    ]
    with (out_dir / "results.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    labels = args.labels or list(DEFAULT_LABELS)
    batch_sizes = args.batch_sizes or [1, 8]
    devices = args.pytorch_device or ["mps", "cpu"]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    if not args.skip_pytorch:
        for device in devices:
            for batch_size in batch_sizes:
                try:
                    row = run_pytorch(
                        args.pytorch_model_dir,
                        args.text,
                        labels,
                        batch_size,
                        device,
                        args.warmup_iters,
                        args.measure_iters,
                    )
                except Exception as exc:
                    row = {
                        "runner": f"pytorch_{device}",
                        "backend": device,
                        "batch_size": batch_size,
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                rows.append(row)
                write_outputs(out_dir, rows)

    if not args.skip_zig:
        for batch_size in batch_sizes:
            if not args.skip_native:
                rows.append(run_zig(args, labels, batch_size, "native", None, out_dir))
                write_outputs(out_dir, rows)
            if not args.skip_metal:
                rows.append(run_zig(args, labels, batch_size, "metal", None, out_dir))
                write_outputs(out_dir, rows)
                if not args.skip_full_graph:
                    rows.append(run_zig(args, labels, batch_size, "metal", "partitioned", out_dir))
                    write_outputs(out_dir, rows)

    write_outputs(out_dir, rows)
    print(out_dir / "results.csv")
    return 1 if any(row.get("status") == "failed" for row in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
