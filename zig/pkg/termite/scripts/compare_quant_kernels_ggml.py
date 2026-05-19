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

"""Run termite quant benchmarks beside a local ggml/llama.cpp benchmark.

This intentionally does not download or build ggml.  Pass a locally built
ggml/llama.cpp command after ``--ggml-command`` and the script captures both
outputs into one timestamped directory so kernel comparisons are reproducible.
Stock llama.cpp does not currently provide a known GLiNER2 model runner, so
GLiNER can be captured two ways: termite's end-to-end native GLiNER2 bench via
``--include-gliner``, or direct ggml quantized matmul kernels shaped like
GLiNER and CLIP/CLAP via ``--include-ggml-gliner-kernels``.

Examples:
  python3 pkg/termite/scripts/compare_quant_kernels_ggml.py \
    --termite-dir pkg/termite \
    --out /tmp/termite-ggml-quant-compare \
    --ggml-command /path/to/llama-bench -m /path/to/model.gguf -ngl 0

  python3 pkg/termite/scripts/compare_quant_kernels_ggml.py \
    --include-gliner \
    --skip-ggml \
    --out /tmp/termite-quant-baseline

  python3 pkg/termite/scripts/compare_quant_kernels_ggml.py \
    --termite-dir pkg/termite \
    --out /tmp/termite-ggml-q4-q5-q8-smoke \
    --skip-ggml \
    --include-ggml-gliner-kernels \
    --native-args "--only-gliner-quant --types Q4_K,Q5_K,Q8_0 --rows 128 --in-dim 768 --out-dim 768" \
    --ggml-gliner-kernel-args "--types Q4_K,Q5_K,Q8_0 --rows 128 --in-dim 768 --out-dim 768"
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path


def run_command(cmd: list[str], cwd: Path | None, env: dict[str, str]) -> dict[str, object]:
    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        exit_code = proc.returncode
        output = proc.stdout
    except OSError as err:
        exit_code = 127
        output = f"failed to start command: {err}\n"
    finished = time.time()
    return {
        "cmd": cmd,
        "cwd": str(cwd) if cwd else None,
        "exit_code": exit_code,
        "elapsed_sec": finished - started,
        "output": output,
    }


def write_result(path: Path, result: dict[str, object]) -> None:
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    path.with_suffix(".log").write_text(str(result["output"]), encoding="utf-8")


TERMITE_GLINER_RE = re.compile(
    r"^nativeGliner directQuant_vs_dequantSgemm "
    r"(?P<type>\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+)\s+"
    r"(?P<direct_ns>\d+)\s+(?P<dequant_ns>\d+)\s+"
)

TERMITE_GLINER_LABEL_RE = re.compile(
    r"^nativeGliner directQuant_vs_dequantSgemm "
    r"(?P<type>\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+)$"
)

TERMITE_GLINER_DIRECT_DEFAULT_RE = re.compile(
    r"^nativeGliner directVariants "
    r"(?P<type>\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+) "
    r"directDefault_vs_dequantSgemm\s+"
    r"(?P<direct_ns>\d+)\s+(?P<dequant_ns>\d+)\s+"
)

TERMITE_GLINER_DIRECT_DEFAULT_LABEL_RE = re.compile(
    r"^nativeGliner directVariants "
    r"(?P<type>\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+) "
    r"directDefault_vs_dequantSgemm$"
)

TERMITE_PACKED_QKV_RE = re.compile(
    r"^packedQKVDirect "
    r"(?P<type>\S+) (?:custom|\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+) "
    r"autoSelector\[[^\]]+\]_vs_dequantSgemmTriple\s+"
    r"(?P<direct_ns>\d+)\s+(?P<dequant_ns>\d+)\s+"
)

TERMITE_PACKED_QKV_LABEL_RE = re.compile(
    r"^packedQKVDirect "
    r"(?P<type>\S+) (?:custom|\S+) (?P<rows>\d+)x(?P<in_dim>\d+)x(?P<out_dim>\d+) "
    r"autoSelector\[[^\]]+\]_vs_dequantSgemmTriple$"
)

TERMITE_LINEAR_PHASE_RE = re.compile(
    r"^quantLinearPhase "
    r"(?P<label>.*?) "
    r"measured_ns=(?P<measured_ns>\d+) "
    r"q8k_alloc_ns=(?P<q8k_alloc_ns>\d+) "
    r"q8k_quant_ns=(?P<q8k_quant_ns>\d+) "
    r"(?:q8_0_alloc_ns=(?P<q8_0_alloc_ns>\d+) "
    r"q8_0_quant_ns=(?P<q8_0_quant_ns>\d+) "
    r"q8_0_compute_ns=(?P<q8_0_compute_ns>\d+) )?"
    r"(?:legacy_alloc_ns=(?P<legacy_alloc_ns>\d+) "
    r"legacy_quant_ns=(?P<legacy_quant_ns>\d+) "
    r"legacy_compute_ns=(?P<legacy_compute_ns>\d+) )?"
    r"q4q5_compute_ns=(?P<q4q5_compute_ns>\d+) "
    r"dequant_fetch_ns=(?P<dequant_fetch_ns>\d+) "
    r"dequant_sgemm_compute_ns=(?P<dequant_sgemm_compute_ns>\d+)"
)

TERMITE_TRIPLE_PHASE_RE = re.compile(
    r"^quantTriplePhase "
    r"(?P<label>.*?) "
    r"measured_ns=(?P<measured_ns>\d+) "
    r"q8k_alloc_ns=(?P<q8k_alloc_ns>\d+) "
    r"q8k_quant_ns=(?P<q8k_quant_ns>\d+) "
    r"q4q5_compute_ns=(?P<q4q5_compute_ns>\d+) "
    r"q4q5_pair_compute_ns=(?P<q4q5_pair_compute_ns>\d+) "
    r"q4q5_triple_compute_ns=(?P<q4q5_triple_compute_ns>\d+) "
    r"dequant_fetch_ns=(?P<dequant_fetch_ns>\d+) "
    r"dequant_sgemm_compute_ns=(?P<dequant_sgemm_compute_ns>\d+) "
    r"packed_mr8=(?P<packed_mr8>\d+) "
    r"packed_mr4=(?P<packed_mr4>\d+) "
    r"packed_mr2=(?P<packed_mr2>\d+) "
    r"packed_mr1=(?P<packed_mr1>\d+)"
)

GGML_GLINER_RE = re.compile(
    r"^ggml_gliner_kernel .*type=(?P<type>\S+) rows=(?P<rows>\d+) "
    r"in=(?P<in_dim>\d+) out=(?P<out_dim>\d+) projections=(?P<projections>\d+) "
    r".*avg_ms=(?P<avg_ms>[0-9.]+) min_ms=(?P<min_ms>[0-9.]+)"
)


ComparisonKey = tuple[str, int, int, int, int]


def comparison_key_json(key: ComparisonKey) -> dict[str, int | str]:
    type_name, rows, in_dim, out_dim, projections = key
    return {
        "type": type_name,
        "rows": rows,
        "in_dim": in_dim,
        "out_dim": out_dim,
        "projections": projections,
    }


def parse_termite_gliner_rows(output: str) -> dict[ComparisonKey, dict[str, float | str]]:
    rows: dict[ComparisonKey, dict[str, float | str]] = {}
    for line in output.splitlines():
        stripped = line.strip()
        parsed = parse_termite_line_key(stripped)
        if parsed is None:
            continue
        key, source, match = parsed
        direct_ns = int(match.group("direct_ns"))
        dequant_ns = int(match.group("dequant_ns"))
        if key in rows and rows[key].get("termite_source") == "nativeGliner" and source == "nativeGlinerDirectDefault":
            continue
        rows[key] = {
            "termite_source": source,
            "termite_direct_ms": direct_ns / 1.0e6,
            "termite_dequant_sgemm_ms": dequant_ns / 1.0e6,
        }
    phase_rows = parse_termite_linear_phase_rows(output)
    merge_phase_rows(phase_rows, parse_termite_triple_phase_rows(output))
    for key, source_phases in phase_rows.items():
        if key not in rows:
            continue
        source = str(rows[key].get("termite_source", ""))
        if source in source_phases:
            rows[key].update(source_phases[source])
    return rows


def merge_phase_rows(
    target: dict[ComparisonKey, dict[str, dict[str, float]]],
    source: dict[ComparisonKey, dict[str, dict[str, float]]],
) -> None:
    for key, source_phases in source.items():
        target_phases = target.setdefault(key, {})
        for phase_source, phase in source_phases.items():
            target_phases[phase_source] = phase


def parse_termite_line_key(line: str) -> tuple[ComparisonKey, str, re.Match[str]] | None:
    match = TERMITE_GLINER_RE.match(line)
    projections = 1
    source = "nativeGliner"
    if not match:
        match = TERMITE_GLINER_DIRECT_DEFAULT_RE.match(line)
        source = "nativeGlinerDirectDefault"
    if not match:
        match = TERMITE_PACKED_QKV_RE.match(line)
        projections = 3
        source = "packedQKVDirect"
    if not match:
        return None
    key = (
        match.group("type"),
        int(match.group("rows")),
        int(match.group("in_dim")),
        int(match.group("out_dim")),
        projections,
    )
    return key, source, match


def parse_termite_label_key(line: str) -> tuple[ComparisonKey, str] | None:
    match = TERMITE_GLINER_LABEL_RE.match(line)
    projections = 1
    source = "nativeGliner"
    if not match:
        match = TERMITE_GLINER_DIRECT_DEFAULT_LABEL_RE.match(line)
        source = "nativeGlinerDirectDefault"
    if not match:
        match = TERMITE_PACKED_QKV_LABEL_RE.match(line)
        projections = 3
        source = "packedQKVDirect"
    if not match:
        return None
    key = (
        match.group("type"),
        int(match.group("rows")),
        int(match.group("in_dim")),
        int(match.group("out_dim")),
        projections,
    )
    return key, source


def parse_termite_linear_phase_rows(output: str) -> dict[ComparisonKey, dict[str, dict[str, float]]]:
    rows: dict[ComparisonKey, dict[str, dict[str, float]]] = {}
    for line in output.splitlines():
        match = TERMITE_LINEAR_PHASE_RE.match(line.strip())
        if not match:
            continue
        parsed = parse_termite_label_key(match.group("label"))
        if parsed is None:
            continue
        key, source = parsed
        phase = {
            "termite_phase_measured_ms": int(match.group("measured_ns")) / 1.0e6,
            "termite_phase_q8k_alloc_ms": int(match.group("q8k_alloc_ns")) / 1.0e6,
            "termite_phase_q8k_quant_ms": int(match.group("q8k_quant_ns")) / 1.0e6,
            "termite_phase_q8_0_alloc_ms": int(match.group("q8_0_alloc_ns") or 0) / 1.0e6,
            "termite_phase_q8_0_quant_ms": int(match.group("q8_0_quant_ns") or 0) / 1.0e6,
            "termite_phase_q8_0_compute_ms": int(match.group("q8_0_compute_ns") or 0) / 1.0e6,
            "termite_phase_legacy_alloc_ms": int(match.group("legacy_alloc_ns") or 0) / 1.0e6,
            "termite_phase_legacy_quant_ms": int(match.group("legacy_quant_ns") or 0) / 1.0e6,
            "termite_phase_legacy_compute_ms": int(match.group("legacy_compute_ns") or 0) / 1.0e6,
            "termite_phase_q4q5_compute_ms": int(match.group("q4q5_compute_ns")) / 1.0e6,
            "termite_phase_dequant_fetch_ms": int(match.group("dequant_fetch_ns")) / 1.0e6,
            "termite_phase_dequant_sgemm_compute_ms": int(match.group("dequant_sgemm_compute_ns")) / 1.0e6,
        }
        rows.setdefault(key, {})[source] = phase
    return rows


def parse_termite_triple_phase_rows(output: str) -> dict[ComparisonKey, dict[str, dict[str, float]]]:
    rows: dict[ComparisonKey, dict[str, dict[str, float]]] = {}
    for line in output.splitlines():
        match = TERMITE_TRIPLE_PHASE_RE.match(line.strip())
        if not match:
            continue
        parsed = parse_termite_label_key(match.group("label"))
        if parsed is None:
            continue
        key, source = parsed
        phase = {
            "termite_phase_measured_ms": int(match.group("measured_ns")) / 1.0e6,
            "termite_phase_q8k_alloc_ms": int(match.group("q8k_alloc_ns")) / 1.0e6,
            "termite_phase_q8k_quant_ms": int(match.group("q8k_quant_ns")) / 1.0e6,
            "termite_phase_q4q5_compute_ms": int(match.group("q4q5_compute_ns")) / 1.0e6,
            "termite_phase_q4q5_pair_compute_ms": int(match.group("q4q5_pair_compute_ns")) / 1.0e6,
            "termite_phase_q4q5_triple_compute_ms": int(match.group("q4q5_triple_compute_ns")) / 1.0e6,
            "termite_phase_dequant_fetch_ms": int(match.group("dequant_fetch_ns")) / 1.0e6,
            "termite_phase_dequant_sgemm_compute_ms": int(match.group("dequant_sgemm_compute_ns")) / 1.0e6,
            "termite_phase_packed_mr8": int(match.group("packed_mr8")),
            "termite_phase_packed_mr4": int(match.group("packed_mr4")),
            "termite_phase_packed_mr2": int(match.group("packed_mr2")),
            "termite_phase_packed_mr1": int(match.group("packed_mr1")),
        }
        rows.setdefault(key, {})[source] = phase
    return rows


def parse_ggml_gliner_rows(output: str) -> dict[ComparisonKey, dict[str, float | int]]:
    rows: dict[ComparisonKey, dict[str, float | int]] = {}
    for line in output.splitlines():
        match = GGML_GLINER_RE.match(line.strip())
        if not match:
            continue
        key = (
            match.group("type"),
            int(match.group("rows")),
            int(match.group("in_dim")),
            int(match.group("out_dim")),
            int(match.group("projections")),
        )
        rows[key] = {
            "ggml_avg_ms": float(match.group("avg_ms")),
            "ggml_min_ms": float(match.group("min_ms")),
        }
    return rows


def add_phase_fractions(row: dict[str, object]) -> None:
    measured = row.get("termite_phase_measured_ms")
    if not isinstance(measured, (int, float)) or measured <= 0:
        return
    phase_keys = [
        "termite_phase_q8k_alloc_ms",
        "termite_phase_q8k_quant_ms",
        "termite_phase_q8_0_alloc_ms",
        "termite_phase_q8_0_quant_ms",
        "termite_phase_q8_0_compute_ms",
        "termite_phase_legacy_alloc_ms",
        "termite_phase_legacy_quant_ms",
        "termite_phase_legacy_compute_ms",
        "termite_phase_q4q5_compute_ms",
        "termite_phase_q4q5_pair_compute_ms",
        "termite_phase_q4q5_triple_compute_ms",
        "termite_phase_dequant_fetch_ms",
        "termite_phase_dequant_sgemm_compute_ms",
    ]
    for key in phase_keys:
        value = row.get(key)
        if isinstance(value, (int, float)):
            fraction_key = key.removesuffix("_ms") + "_fraction"
            row[fraction_key] = value / measured


def worst_ratio_row(comparisons: list[dict[str, object]], ratio_key: str) -> dict[str, object] | None:
    worst: dict[str, object] | None = None
    worst_ratio = -1.0
    for comparison in comparisons:
        ratio = comparison.get(ratio_key)
        if not isinstance(ratio, (int, float)):
            continue
        if float(ratio) > worst_ratio:
            worst_ratio = float(ratio)
            worst = comparison
    return worst


def write_comparison_summary(out_dir: Path, manifest: dict[str, object], args: argparse.Namespace | None = None) -> list[dict[str, object]]:
    ggml_path = out_dir / "ggml_gliner_kernels.json"
    if not ggml_path.exists():
        manifest["comparison_rows"] = 0
        return []

    ggml_result = json.loads(ggml_path.read_text(encoding="utf-8"))
    termite_rows: dict[ComparisonKey, dict[str, float | str]] = {}
    for termite_path in (out_dir / "termite_gliner_quant_kernels.json", out_dir / "termite_clipclap_kernels.json"):
        if not termite_path.exists():
            continue
        termite_result = json.loads(termite_path.read_text(encoding="utf-8"))
        termite_rows.update(parse_termite_gliner_rows(str(termite_result.get("output", ""))))
    ggml_rows = parse_ggml_gliner_rows(str(ggml_result.get("output", "")))
    matched_keys = termite_rows.keys() & ggml_rows.keys()
    unmatched_termite = [comparison_key_json(key) for key in sorted(termite_rows.keys() - ggml_rows.keys())]
    unmatched_ggml = [comparison_key_json(key) for key in sorted(ggml_rows.keys() - termite_rows.keys())]

    comparisons = []
    for key in sorted(matched_keys):
        type_name, rows, in_dim, out_dim, projections = key
        termite = termite_rows[key]
        ggml = ggml_rows[key]
        termite_direct_ms = float(termite["termite_direct_ms"])
        termite_dequant_ms = float(termite["termite_dequant_sgemm_ms"])
        ggml_avg_ms = float(ggml["ggml_avg_ms"])
        comparison = {
            "type": type_name,
            "rows": rows,
            "in_dim": in_dim,
            "out_dim": out_dim,
            "projections": projections,
            **termite,
            **ggml,
            "termite_direct_vs_ggml_avg": termite_direct_ms / ggml_avg_ms if ggml_avg_ms else None,
            "termite_direct_vs_dequant_sgemm": termite_direct_ms / termite_dequant_ms if termite_dequant_ms else None,
        }
        add_phase_fractions(comparison)
        comparisons.append(comparison)

    manifest["comparison_rows"] = len(comparisons)
    if not comparisons:
        summary_path = out_dir / "comparison_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "comparisons": [],
                    "gate_failures": [],
                    "worst_termite_ggml_gap": None,
                    "worst_termite_sgemm_gap": None,
                    "unmatched_termite": unmatched_termite,
                    "unmatched_ggml": unmatched_ggml,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        results = manifest.setdefault("results", [])
        if isinstance(results, list) and "comparison_summary.json" not in results:
            results.append("comparison_summary.json")
        manifest["comparison_gate_failures"] = 0
        manifest["worst_termite_ggml_gap"] = None
        manifest["worst_termite_sgemm_gap"] = None
        return []
    gate_failures = []
    max_ggml_ratio = getattr(args, "max_termite_ggml_ratio", None) if args is not None else None
    max_sgemm_ratio = getattr(args, "max_termite_sgemm_ratio", None) if args is not None else None
    for comparison in comparisons:
        ggml_ratio = comparison.get("termite_direct_vs_ggml_avg")
        if max_ggml_ratio is not None and ggml_ratio is not None and float(ggml_ratio) > float(max_ggml_ratio):
            gate_failures.append({ "gate": "max_termite_ggml_ratio", "limit": max_ggml_ratio, **comparison })
        sgemm_ratio = comparison.get("termite_direct_vs_dequant_sgemm")
        if max_sgemm_ratio is not None and sgemm_ratio is not None and float(sgemm_ratio) > float(max_sgemm_ratio):
            gate_failures.append({ "gate": "max_termite_sgemm_ratio", "limit": max_sgemm_ratio, **comparison })

    summary_path = out_dir / "comparison_summary.json"
    worst_ggml_gap = worst_ratio_row(comparisons, "termite_direct_vs_ggml_avg")
    worst_sgemm_gap = worst_ratio_row(comparisons, "termite_direct_vs_dequant_sgemm")
    summary_path.write_text(
        json.dumps(
            {
                "comparisons": comparisons,
                "gate_failures": gate_failures,
                "worst_termite_ggml_gap": worst_ggml_gap,
                "worst_termite_sgemm_gap": worst_sgemm_gap,
                "unmatched_termite": unmatched_termite,
                "unmatched_ggml": unmatched_ggml,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    results = manifest.setdefault("results", [])
    if isinstance(results, list) and "comparison_summary.json" not in results:
        results.append("comparison_summary.json")
    manifest["comparison_gate_failures"] = len(gate_failures)
    manifest["worst_termite_ggml_gap"] = worst_ggml_gap
    manifest["worst_termite_sgemm_gap"] = worst_sgemm_gap
    return gate_failures


def fail_after_result(out_dir: Path, manifest: dict[str, object], label: str, result: dict[str, object]) -> int | None:
    exit_code = int(result["exit_code"])
    if exit_code == 0:
        return None
    write_comparison_summary(out_dir, manifest)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"{label} failed with exit code {exit_code}; wrote artifacts to {out_dir}", file=sys.stderr)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--termite-dir", default="pkg/termite", help="Path to the termite package directory.")
    parser.add_argument("--out", help="Output directory for captured JSON/log files.")
    parser.add_argument("--skip-native", action="store_true", help="Skip termite native benchmarks.")
    parser.add_argument("--skip-ggml", action="store_true", help="Skip the external ggml command.")
    parser.add_argument("--include-clipclap", action="store_true", default=True, help="Capture CLIP/CLAP termite benches.")
    parser.add_argument("--no-clipclap", dest="include_clipclap", action="store_false", help="Do not capture CLIP/CLAP termite benches.")
    parser.add_argument("--include-gliner", action="store_true", help="Capture termite's end-to-end GLiNER2 native bench.")
    parser.add_argument(
        "--include-ggml-gliner-kernels",
        action="store_true",
        help="Compile and run the bundled ggml GLiNER and CLIP/CLAP-shaped quantized matmul benchmark.",
    )
    parser.add_argument(
        "--native-args",
        default="--warmup-iters 1 --measure-iters 3",
        help="Arguments passed to bench-clipclap-kernels after zig build --. For kernel-only GLiNER smokes, use --only-gliner-quant with --kind Q4_K or --types Q4_K,Q5_K,Q8_0.",
    )
    parser.add_argument(
        "--native-dispatch-stats",
        action="store_true",
        help="Build bench-clipclap-kernels with quant dispatch timing counters. Useful for diagnostics, but it adds atomic counter overhead to native timings.",
    )
    parser.add_argument(
        "--native-model-args",
        default="--target clip_text --clip-text-layers 12 --seq-len 77 --quant q5_k --warmup-iters 1 --measure-iters 3",
        help="Arguments passed to bench-clipclap-native after zig build --.",
    )
    parser.add_argument(
        "--native-gliner-args",
        default="--seq-len 256 --num-layers 12 --num-labels 8 --quant q5_k --warmup-iters 1 --measure-iters 3",
        help="Arguments passed to bench-gliner2-native after zig build --.",
    )
    parser.add_argument(
        "--ggml-gliner-kernel-args",
        default="--warmup-iters 1 --measure-iters 5 --threads 4",
        help="Arguments passed to the bundled ggml_gliner_kernel_bench executable. Supports filters such as --type Q4_K or --types Q4_K,Q5_K,Q8_0 with --rows 77 --in-dim 768 --out-dim 768, plus --projections 3 for qkv-style sibling projections.",
    )
    parser.add_argument(
        "--ggml-prefix",
        default=os.environ.get("GGML_PREFIX", "/opt/homebrew/opt/ggml"),
        help="ggml install prefix used to compile the bundled kernel harness.",
    )
    parser.add_argument(
        "--ggml-cpu-plugin",
        default=os.environ.get("GGML_CPU_PLUGIN"),
        help="Optional explicit ggml CPU plugin path for the bundled kernel harness.",
    )
    parser.add_argument(
        "--ggml-command",
        nargs=argparse.REMAINDER,
        help="External ggml/llama.cpp benchmark command. Put this option last.",
    )
    parser.add_argument(
        "--max-termite-ggml-ratio",
        type=float,
        help="Fail if any matched Termite direct kernel is slower than this ratio versus ggml average time.",
    )
    parser.add_argument(
        "--max-termite-sgemm-ratio",
        type=float,
        help="Fail if any matched Termite direct kernel is slower than this ratio versus cached dequant+SGEMM time.",
    )
    parser.add_argument(
        "--min-comparison-rows",
        type=int,
        default=0,
        help="Fail if fewer than this many Termite/ggml comparison rows are produced.",
    )
    parser.add_argument(
        "--self-test-parsers",
        action="store_true",
        help="Run parser self-tests and exit without running benchmarks.",
    )
    return parser.parse_args()


def self_test_parsers() -> None:
    q8_output = "\n".join(
        [
            "nativeGliner directQuant_vs_dequantSgemm Q8_0 128x3072x768 1270000 792000 1.60x 0.6886",
            "quantLinearPhase nativeGliner directQuant_vs_dequantSgemm Q8_0 128x3072x768 measured_ns=1270000 q8k_alloc_ns=0 q8k_quant_ns=0 q8_0_alloc_ns=0 q8_0_quant_ns=69000 q8_0_compute_ns=1200000 q4q5_compute_ns=0 dequant_fetch_ns=0 dequant_sgemm_compute_ns=0",
        ]
    )
    q8_rows = parse_termite_gliner_rows(q8_output)
    q8 = q8_rows[("Q8_0", 128, 3072, 768, 1)]
    assert q8["termite_source"] == "nativeGliner"
    assert q8["termite_phase_q8_0_quant_ms"] == 0.069
    assert q8["termite_phase_q8_0_compute_ms"] == 1.2
    assert worst_ratio_row([q8], "termite_phase_q8_0_compute_ms") is q8

    duplicate_output = "\n".join(
        [
            "nativeGliner directQuant_vs_dequantSgemm Q4_K 128x768x768 292500 116500 2.51x -1.8130",
            "quantLinearPhase nativeGliner directQuant_vs_dequantSgemm Q4_K 128x768x768 measured_ns=292500 q8k_alloc_ns=0 q8k_quant_ns=31000 q8_0_alloc_ns=0 q8_0_quant_ns=0 q8_0_compute_ns=0 q4q5_compute_ns=260500 dequant_fetch_ns=0 dequant_sgemm_compute_ns=0",
            "nativeGliner directVariants Q4_K 128x768x768 directDefault_vs_dequantSgemm 354500 113000 3.14x -1.5367",
            "quantLinearPhase nativeGliner directVariants Q4_K 128x768x768 directDefault_vs_dequantSgemm measured_ns=354500 q8k_alloc_ns=500 q8k_quant_ns=39500 q8_0_alloc_ns=0 q8_0_quant_ns=0 q8_0_compute_ns=0 q4q5_compute_ns=314500 dequant_fetch_ns=0 dequant_sgemm_compute_ns=0",
        ]
    )
    duplicate_rows = parse_termite_gliner_rows(duplicate_output)
    duplicate = duplicate_rows[("Q4_K", 128, 768, 768, 1)]
    assert duplicate["termite_source"] == "nativeGliner"
    assert duplicate["termite_direct_ms"] == 0.2925
    assert duplicate["termite_phase_q4q5_compute_ms"] == 0.2605

    legacy_output = "\n".join(
        [
            "nativeGliner directQuant_vs_dequantSgemm Q4_0 128x768x768 592000 151000 3.92x -1.8130",
            "quantLinearPhase nativeGliner directQuant_vs_dequantSgemm Q4_0 128x768x768 measured_ns=592000 q8k_alloc_ns=0 q8k_quant_ns=0 q8_0_alloc_ns=0 q8_0_quant_ns=0 q8_0_compute_ns=0 legacy_alloc_ns=5000 legacy_quant_ns=92000 legacy_compute_ns=489000 q4q5_compute_ns=0 dequant_fetch_ns=0 dequant_sgemm_compute_ns=0",
        ]
    )
    legacy_rows = parse_termite_gliner_rows(legacy_output)
    legacy = legacy_rows[("Q4_0", 128, 768, 768, 1)]
    assert legacy["termite_phase_legacy_alloc_ms"] == 0.005
    assert legacy["termite_phase_legacy_quant_ms"] == 0.092
    assert legacy["termite_phase_legacy_compute_ms"] == 0.489

    packed_output = "\n".join(
        [
            "packedQKVDirect Q5_K custom 77x768x768 autoSelector[packedMR8+MR4+MR1]_vs_dequantSgemmTriple 471000 409500 1.15x 1.0619",
            "quantTriplePhase packedQKVDirect Q5_K custom 77x768x768 autoSelector[packedMR8+MR4+MR1]_vs_dequantSgemmTriple measured_ns=471000 q8k_alloc_ns=0 q8k_quant_ns=0 q4q5_compute_ns=0 q4q5_pair_compute_ns=0 q4q5_triple_compute_ns=0 dequant_fetch_ns=0 dequant_sgemm_compute_ns=0 packed_mr8=1 packed_mr4=1 packed_mr2=0 packed_mr1=1",
        ]
    )
    packed_rows = parse_termite_gliner_rows(packed_output)
    packed = packed_rows[("Q5_K", 77, 768, 768, 3)]
    assert packed["termite_source"] == "packedQKVDirect"
    assert packed["termite_phase_packed_mr8"] == 1
    assert packed["termite_phase_packed_mr4"] == 1
    assert packed["termite_phase_packed_mr1"] == 1


def main() -> int:
    args = parse_args()
    if args.self_test_parsers:
        self_test_parsers()
        print("parser self-tests passed")
        return 0
    if not args.out:
        print("--out is required unless --self-test-parsers is used", file=sys.stderr)
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    termite_dir = Path(args.termite_dir)

    env = os.environ.copy()
    env.setdefault("ZIG_GLOBAL_CACHE_DIR", str((out_dir / "zig-global-cache").resolve()))
    env.setdefault("ZIG_LOCAL_CACHE_DIR", str((out_dir / "zig-local-cache").resolve()))
    env.setdefault("GGML_PREFIX", str(Path(args.ggml_prefix).resolve()))
    if args.ggml_cpu_plugin:
        env["GGML_CPU_PLUGIN"] = str(Path(args.ggml_cpu_plugin).resolve())

    manifest: dict[str, object] = {
        "created_unix": time.time(),
        "termite_dir": str(termite_dir),
        "native_dispatch_stats": args.native_dispatch_stats,
        "comparison_gates": {
            "max_termite_ggml_ratio": args.max_termite_ggml_ratio,
            "max_termite_sgemm_ratio": args.max_termite_sgemm_ratio,
            "min_comparison_rows": args.min_comparison_rows,
        },
        "results": [],
    }
    native_args = shlex.split(args.native_args)
    native_kernel_is_gliner_quant = "--only-gliner-quant" in native_args
    native_kernel_only = native_kernel_is_gliner_quant or "--only-packed-qkv" in native_args

    if not args.skip_native and args.include_clipclap:
        native_kernel_cmd = [
            "zig",
            "build",
            "bench-clipclap-kernels",
            "-Dskip-openapi=true",
            "-Donnx=false",
            "-Dmlx=false",
            "-Dmetal=false",
        ]
        if args.native_dispatch_stats:
            native_kernel_cmd.append("-Denable-native-quant-dispatch-stats=true")
        native_kernel_cmd.extend(["--", *native_args])
        result = run_command(native_kernel_cmd, termite_dir, env)
        native_kernel_result = "termite_gliner_quant_kernels.json" if native_kernel_is_gliner_quant else "termite_clipclap_kernels.json"
        write_result(out_dir / native_kernel_result, result)
        manifest["results"].append(native_kernel_result)
        failed = fail_after_result(out_dir, manifest, "termite CLIP/CLAP kernel benchmark", result)
        if failed is not None:
            return failed

    if not args.skip_native and args.include_clipclap and not native_kernel_only:
        native_model_cmd = [
            "zig",
            "build",
            "bench-clipclap-native",
            "-Dskip-openapi=true",
            "-Donnx=false",
            "-Dmlx=false",
            "-Dmetal=false",
            "--",
            *shlex.split(args.native_model_args),
        ]
        result = run_command(native_model_cmd, termite_dir, env)
        write_result(out_dir / "termite_clipclap_native.json", result)
        manifest["results"].append("termite_clipclap_native.json")
        failed = fail_after_result(out_dir, manifest, "termite CLIP/CLAP native benchmark", result)
        if failed is not None:
            return failed

    if not args.skip_native and args.include_gliner:
        native_gliner_cmd = [
            "zig",
            "build",
            "bench-gliner2-native",
            "-Dskip-openapi=true",
            "-Donnx=false",
            "-Dmlx=false",
            "-Dmetal=false",
            "--",
            *shlex.split(args.native_gliner_args),
        ]
        result = run_command(native_gliner_cmd, termite_dir, env)
        write_result(out_dir / "termite_gliner2_native.json", result)
        manifest["results"].append("termite_gliner2_native.json")
        failed = fail_after_result(out_dir, manifest, "termite GLiNER2 native benchmark", result)
        if failed is not None:
            return failed

    if args.include_ggml_gliner_kernels:
        ggml_prefix = Path(args.ggml_prefix)
        ggml_include = ggml_prefix / "include"
        ggml_lib = ggml_prefix / "lib"
        src = termite_dir / "scripts" / "ggml_gliner_kernel_bench.c"
        exe = out_dir / "ggml_gliner_kernel_bench"
        compile_cmd = [
            "cc",
            "-O3",
            "-DNDEBUG",
            f"-I{ggml_include}",
            str(src),
            f"-L{ggml_lib}",
            "-lggml",
            "-lggml-base",
            f"-Wl,-rpath,{ggml_lib}",
            "-o",
            str(exe),
        ]
        result = run_command(compile_cmd, None, env)
        write_result(out_dir / "ggml_gliner_kernel_compile.json", result)
        manifest["results"].append("ggml_gliner_kernel_compile.json")
        failed = fail_after_result(out_dir, manifest, "ggml GLiNER kernel benchmark compile", result)
        if failed is not None:
            return failed

        run_cmd = [str(exe), *shlex.split(args.ggml_gliner_kernel_args)]
        result = run_command(run_cmd, None, env)
        write_result(out_dir / "ggml_gliner_kernels.json", result)
        manifest["results"].append("ggml_gliner_kernels.json")
        failed = fail_after_result(out_dir, manifest, "ggml GLiNER kernel benchmark", result)
        if failed is not None:
            return failed
        write_comparison_summary(out_dir, manifest, args)

    if not args.skip_ggml:
        if not args.ggml_command:
            print("missing --ggml-command; use --skip-ggml to capture termite-only baselines", file=sys.stderr)
            return 2
        result = run_command(args.ggml_command, None, env)
        write_result(out_dir / "ggml.json", result)
        manifest["results"].append("ggml.json")
        failed = fail_after_result(out_dir, manifest, "external ggml benchmark", result)
        if failed is not None:
            return failed

    gates_requested = args.max_termite_ggml_ratio is not None or args.max_termite_sgemm_ratio is not None
    comparison_rows = int(manifest.get("comparison_rows", 0))
    if args.min_comparison_rows > 0 and comparison_rows < args.min_comparison_rows:
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(
            f"expected at least {args.min_comparison_rows} comparison row(s), found {comparison_rows}; wrote artifacts to {out_dir}",
            file=sys.stderr,
        )
        return 3
    if gates_requested and comparison_rows == 0:
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"comparison gates requested but no matched Termite/ggml rows were found; wrote artifacts to {out_dir}", file=sys.stderr)
        return 3
    gate_failures = manifest.get("comparison_gate_failures", 0)
    if isinstance(gate_failures, int) and gate_failures > 0:
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"comparison gates failed for {gate_failures} row(s); wrote artifacts to {out_dir}", file=sys.stderr)
        return 3

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote comparison artifacts to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
