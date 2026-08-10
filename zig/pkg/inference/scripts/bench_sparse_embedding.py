#!/usr/bin/env python3
"""Measure real sparse embedding batches without adding timing gates to tests.

Example:
  python3 pkg/inference/scripts/bench_sparse_embedding.py \
    /path/to/splade-model --repeats 5 --build-label ci-x86_64-baseline
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
from pathlib import Path


TEXTS = [
    "machine learning model",
    "machine learning model",
    "machine learning algorithms",
    "training a learning model",
    "banana bread recipe",
    "garden tomato plants",
    "weather forecast rain",
    "tomorrow rain weather forecast",
    "neural network training",
]
TIMING_RE = re.compile(r"timing_ms: .*\btext=(\d+)\b.*\btotal=(\d+)\b")


def cpu_model() -> str:
    if platform.system() == "Darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_once(args: argparse.Namespace, batch: int) -> tuple[int, int]:
    command = [
        str(args.antfly_bin),
        "inference",
        "embed",
        str(args.model_dir),
        "--backend",
        args.backend,
        "--graph-runtime",
        args.graph_runtime,
        "--print-timing",
    ]
    for text in TEXTS[:batch]:
        command.extend(("--text", text))
    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    match = TIMING_RE.search(completed.stderr)
    if match is None:
        raise RuntimeError("antfly did not emit a timing_ms line")
    return int(match.group(1)), int(match.group(2))


def main() -> None:
    zig_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_dir", type=Path)
    parser.add_argument(
        "--antfly-bin", type=Path, default=zig_root / "zig-out/bin/antfly"
    )
    parser.add_argument("--backend", default="native")
    parser.add_argument("--graph-runtime", default="interpreter")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--build-label", default=os.environ.get("ANTFLY_SPARSE_BENCH_BUILD", "unknown")
    )
    args = parser.parse_args()
    if args.warmups < 0 or args.repeats < 1:
        parser.error("--warmups must be >= 0 and --repeats must be >= 1")

    model_path = args.model_dir / "onnx/model.onnx"
    metadata = {
        "event": "sparse_embedding_benchmark_metadata",
        "machine": platform.machine(),
        "system": platform.system(),
        "system_release": platform.release(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "build_label": args.build_label,
        "backend": args.backend,
        "graph_runtime": args.graph_runtime,
        "model_dir": str(args.model_dir.resolve()),
        "model_sha256": sha256(model_path),
        "warmups": args.warmups,
        "repeats": args.repeats,
    }
    print(json.dumps(metadata, sort_keys=True))

    for batch in (1, 3, 9):
        for _ in range(args.warmups):
            run_once(args, batch)
        samples = [run_once(args, batch) for _ in range(args.repeats)]
        text_ms = [sample[0] for sample in samples]
        total_ms = [sample[1] for sample in samples]
        result = {
            "event": "sparse_embedding_benchmark_result",
            "batch": batch,
            "text_ms": text_ms,
            "text_ms_median": statistics.median(text_ms),
            "text_ms_min": min(text_ms),
            "text_ms_max": max(text_ms),
            "total_ms": total_ms,
            "total_ms_median": statistics.median(total_ms),
        }
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
