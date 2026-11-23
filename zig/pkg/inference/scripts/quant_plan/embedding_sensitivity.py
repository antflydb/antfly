#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Output-space sensitivity and evaluation for mixed-precision embedder plans.

`measure` replaces each candidate's data-free weight loss in a sensitivity
file (from `antfly inference export --quantize-sensitivity`) with the
embedding damage of quantizing only that tensor: every other planned tensor
stays dense, so each measurement isolates one tensor and one format. The loss
is the mean of 1 - cos(reference, quantized) over a corpus, summed losses then
approximate the damage of a whole plan (first order).

`evaluate` exports GGUFs for plans or uniform formats and reports the same
cosine loss plus nearest-neighbor recall against the dense reference, so a
plan can be compared to a uniform format at equal size.

Only the Python standard library is used. Example:

    antfly inference export MODEL --target gguf --format q4_k \\
        --quantize-sensitivity weight.json
    embedding_sensitivity.py measure --antfly antfly-inference --model MODEL \\
        --base-format q4_k --sensitivity weight.json --corpus corpus.txt \\
        --output cosine.json
    antfly inference quantize-plan --sensitivity cosine.json \\
        --bits-per-weight 4.5 --output plan.json
    embedding_sensitivity.py evaluate --antfly antfly-inference --model MODEL \\
        --base-format q4_k --corpus corpus.txt --plan plan.json --uniform q4_k,q5_k
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PLAN_SCHEMA = "antfly-quant-plan/v1"
SENSITIVITY_SCHEMA = "antfly-quant-sensitivity/v1"
METRIC = "embedding_cosine_loss"


def read_corpus(path: Path, limit: int) -> list[str]:
    texts = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not texts:
        raise SystemExit(f"{path}: corpus is empty")
    return texts[:limit] if limit > 0 else texts


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-4000:])
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd[:4])} ...")
    return proc.stdout


def embed(antfly: str, model: str, texts: list[str], batch: int) -> list[list[float]]:
    vectors: list[list[float]] = []
    for start in range(0, len(texts), batch):
        cmd = [antfly, "embed", model, "--backend", "native"]
        for text in texts[start : start + batch]:
            cmd += ["--text", text]
        out = run(cmd)
        payload = json.loads(out[out.index("{") :])
        vectors.extend(payload["embeddings"])
    if len(vectors) != len(texts):
        raise SystemExit(f"expected {len(texts)} embeddings, got {len(vectors)}")
    return [normalize(v) for v in vectors]


def normalize(v: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in v))
    if not math.isfinite(norm) or norm == 0:
        raise SystemExit("embedding has zero or non-finite norm")
    return [x / norm for x in v]


def dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def cosine_loss(reference: list[list[float]], candidate: list[list[float]]) -> float:
    return sum(1.0 - dot(r, c) for r, c in zip(reference, candidate)) / len(reference)


def neighbor_recall(
    reference: list[list[float]], candidate: list[list[float]], k: int
) -> float:
    """Mean overlap of each text's top-k neighbors (excluding itself)."""
    n = len(reference)
    k = min(k, n - 1)
    if k <= 0:
        return 1.0

    def topk(vectors: list[list[float]], i: int) -> set[int]:
        scores = [(dot(vectors[i], vectors[j]), j) for j in range(n) if j != i]
        scores.sort(reverse=True)
        return {j for _, j in scores[:k]}

    return sum(len(topk(reference, i) & topk(candidate, i)) / k for i in range(n)) / n


WEIGHT_SUFFIXES = (
    ".safetensors",
    ".bin",
    ".onnx",
    ".onnx.data",
    ".gguf",
    ".pt",
    ".pth",
)


def export(
    antfly: str, model: str, base_format: str, out_dir: Path, plan: dict | None
) -> int:
    """Export a GGUF into `out_dir` beside the source's tokenizer and config
    sidecars (a bare GGUF carries no tokenizer), and return its size."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    for entry in Path(model).iterdir():
        if (
            entry.name.endswith(WEIGHT_SUFFIXES)
            or entry.name == "model.safetensors.index.json"
        ):
            continue
        if entry.is_dir():
            shutil.copytree(entry, out_dir / entry.name)
        else:
            shutil.copy2(entry, out_dir / entry.name)
    gguf = out_dir / "model.gguf"
    cmd = [
        antfly,
        "export",
        model,
        "--target",
        "gguf",
        "--format",
        base_format,
        "--output",
        str(gguf),
    ]
    if plan is not None:
        plan_path = out_dir.parent / f"{out_dir.name}.plan.json"
        plan_path.write_text(json.dumps({"schema": PLAN_SCHEMA, "tensors": plan}))
        cmd += ["--quantize-plan", str(plan_path)]
    run(cmd)
    return gguf.stat().st_size


def load_sensitivity(path: Path) -> dict:
    data = json.loads(path.read_text())
    if data.get("schema") != SENSITIVITY_SCHEMA:
        raise SystemExit(f"{path}: expected schema {SENSITIVITY_SCHEMA}")
    return data


def measured_groups(groups: list[dict], tensors: list[dict]) -> list[dict]:
    """Fused-kernel groups whose members were all measured (others are dropped,
    so a filtered run still allocates; its unmeasured tensors stay at the base
    format)."""
    names = {t["name"] for t in tensors}
    return [g for g in groups if all(m in names for m in g["members"])]


def measure(args: argparse.Namespace) -> None:
    weight = load_sensitivity(Path(args.sensitivity))
    texts = read_corpus(Path(args.corpus), args.limit)
    reference = embed(args.antfly, args.model, texts, args.batch)
    names = [t["name"] for t in weight["tensors"]]
    pattern = re.compile(args.tensors) if args.tensors else None
    formats = set(args.formats.split(",")) if args.formats else None

    measured = []
    with tempfile.TemporaryDirectory(dir=args.workdir) as tmp:
        out = Path(tmp) / "probe"
        for tensor in weight["tensors"]:
            name = tensor["name"]
            if pattern and not pattern.search(name):
                continue
            candidates = []
            for candidate in tensor["candidates"]:
                if formats and candidate["format"] not in formats:
                    continue
                plan = {other: "none" for other in names}
                plan[name] = candidate["format"]
                started = time.time()
                export(args.antfly, args.model, args.base_format, out, plan)
                loss = cosine_loss(
                    reference, embed(args.antfly, str(out), texts, args.batch)
                )
                candidates.append({**candidate, "loss": max(loss, 0.0)})
                print(
                    f"{name} {candidate['format']}: loss {loss:.3e} ({time.time() - started:.1f}s)",
                    file=sys.stderr,
                )
            if candidates:
                measured.append(
                    {
                        "name": name,
                        "elements": tensor["elements"],
                        "candidates": candidates,
                    }
                )

    Path(args.output).write_text(
        json.dumps(
            {
                "schema": SENSITIVITY_SCHEMA,
                "metric": METRIC,
                "tensors": measured,
                "groups": measured_groups(weight.get("groups", []), measured),
            },
            indent=2,
        )
    )
    print(f"wrote {len(measured)} tensors to {args.output}", file=sys.stderr)


def evaluate(args: argparse.Namespace) -> None:
    texts = read_corpus(Path(args.corpus), args.limit)
    reference = embed(args.antfly, args.model, texts, args.batch)
    variants: list[tuple[str, str, dict | None]] = []
    for fmt in filter(None, (args.uniform or "").split(",")):
        variants.append((f"uniform {fmt}", fmt, None))
    for plan_path in args.plan or []:
        plan = json.loads(Path(plan_path).read_text())
        if plan.get("schema") != PLAN_SCHEMA:
            raise SystemExit(f"{plan_path}: expected schema {PLAN_SCHEMA}")
        variants.append(
            (f"plan {Path(plan_path).name}", args.base_format, plan["tensors"])
        )

    rows = []
    with tempfile.TemporaryDirectory(dir=args.workdir) as tmp:
        for label, base, plan in variants:
            out = Path(tmp) / "variant"
            size = export(args.antfly, args.model, base, out, plan)
            vectors = embed(args.antfly, str(out), texts, args.batch)
            rows.append(
                {
                    "variant": label,
                    "file_bytes": size,
                    "cosine_loss": cosine_loss(reference, vectors),
                    "min_cosine": min(dot(r, c) for r, c in zip(reference, vectors)),
                    f"recall@{args.k}": neighbor_recall(reference, vectors, args.k),
                }
            )
            print(json.dumps(rows[-1]), file=sys.stderr)
    print(json.dumps({"texts": len(texts), "results": rows}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("measure", "evaluate"):
        p = sub.add_parser(name)
        p.add_argument(
            "--antfly",
            required=True,
            help="antfly-inference binary (with export/embed commands)",
        )
        p.add_argument(
            "--model",
            required=True,
            help="dense source model directory (the reference)",
        )
        p.add_argument(
            "--base-format",
            required=True,
            help="base --format whose eligible tensors the plan refines",
        )
        p.add_argument(
            "--corpus", required=True, help="UTF-8 text file, one input per line"
        )
        p.add_argument(
            "--limit", type=int, default=0, help="use only the first N corpus lines"
        )
        p.add_argument(
            "--batch", type=int, default=64, help="texts per embed invocation"
        )
        p.add_argument(
            "--workdir",
            default=os.environ.get("TMPDIR"),
            help="directory for temporary GGUFs",
        )
    m = sub.choices["measure"]
    m.add_argument(
        "--sensitivity",
        required=True,
        help="weight sensitivity file listing tensors, formats and bytes",
    )
    m.add_argument("--output", required=True)
    m.add_argument("--tensors", help="regex: only measure matching tensors")
    m.add_argument("--formats", help="csv: only measure these candidate formats")
    e = sub.choices["evaluate"]
    e.add_argument("--plan", action="append", help="plan file (repeatable)")
    e.add_argument("--uniform", help="csv of uniform formats to compare")
    e.add_argument("--k", type=int, default=10)
    args = parser.parse_args()
    {"measure": measure, "evaluate": evaluate}[args.command](args)


if __name__ == "__main__":
    main()
