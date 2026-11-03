#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["pillow"]
# ///
"""Compare an animated GIF with its first frame through the image embedder.

Use build-gemma4-image-bench. Each format gets a fresh process/session, retaining
first-use samples separately from warm samples. File IO, loading, HTTP and base64
are outside the native timing boundary. Stage timers are host elapsed time;
CUDA encoder time includes final readback and any outstanding vision work.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path

from PIL import Image


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    gif = args.gif.resolve()
    with Image.open(gif) as image:
        if image.format != "GIF":
            raise ValueError("expected a GIF input")
        width, height, frames = image.width, image.height, image.n_frames
        image.seek(0)
        image.convert("RGB").save(output / "first.png")
        image.save(output / "first.gif")
    # Saving a palette frame must preserve the input pixels.
    with (
        Image.open(output / "first.png") as png,
        Image.open(output / "first.gif") as still,
    ):
        if png.tobytes() != still.convert("RGB").tobytes():
            raise ValueError("first-frame control pixels differ")
    binary, model = args.binary.resolve(), args.model.resolve()
    report = {
        "version": 1,
        "host": platform.platform(),
        "binary_sha256": sha256(binary),
        "checkpoint_receipt_sha256": sha256(model / "embeddinggemma2_receipt.json"),
        "backend": args.backend,
        "width": width,
        "height": height,
        "frames": frames,
        "timing_scope": "encoded image to synchronized vector; excludes loading, file IO, HTTP and base64",
        "stage_timing_scope": "host elapsed; GPU stages enqueue asynchronously; encoder includes outstanding work and readback on CUDA",
        "environment": {
            k: v
            for k, v in os.environ.items()
            if k.startswith("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_")
        },
        "cases": {},
    }
    reference = None
    for name, path in (
        ("animated_gif", gif),
        ("first_png", output / "first.png"),
        ("first_gif", output / "first.gif"),
    ):
        receipt = output / f"{name}.json"
        command = [
            str(binary),
            "--model",
            str(model),
            "--backend",
            args.backend,
            "--scope",
            "encoded-embedding",
            "--batch",
            "1",
            "--batching",
            "off",
            "--warmups",
            str(args.warmups),
            "--iterations",
            str(args.iterations),
            "--image",
            str(path),
            "--output",
            str(receipt),
        ]
        with (output / f"{name}.log").open("w") as log:
            subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=args.timeout,
            )
        raw = json.loads(receipt.read_text())
        case = raw["cases"][0]
        vector = case["output"]
        if len(vector) != 768 or not all(math.isfinite(v) for v in vector):
            raise ValueError(f"{name}: expected a finite 768-dimensional vector")
        norm = math.sqrt(sum(v * v for v in vector))
        if abs(norm - 1) > 1e-4:
            raise ValueError(f"{name}: vector is not normalized")
        if reference is None:
            reference = vector
        cosine = sum(a * b for a, b in zip(reference, vector)) / math.sqrt(
            sum(a * a for a in reference) * sum(b * b for b in vector)
        )
        max_abs = max(abs(a - b) for a, b in zip(reference, vector))
        if cosine < 0.999999 or max_abs > 1e-6:
            raise ValueError(
                f"{name}: first-frame output differs: {cosine=} {max_abs=}"
            )
        report["cases"][name] = {
            "path": str(path),
            "sha256": sha256(path),
            "bytes": path.stat().st_size,
            "load_seconds": raw["load_seconds"],
            "tokenizer_load_seconds": raw["tokenizer_load_seconds"],
            "first_use_seconds": (case["warmup_samples"] or case["samples"])[0][
                "seconds"
            ],
            "p50_seconds": case["p50_seconds"],
            "p95_seconds": case["p95_seconds"],
            "projected_tokens": case["projected_tokens"],
            "stage_medians_ms": {
                key: statistics.median(s["media"][key] for s in case["samples"]) / 1e6
                for key in (
                    "inspection_ns",
                    "preprocess_ns",
                    "vision_ns",
                    "pack_ns",
                    "encoder_ns",
                    "pool_readback_ns",
                )
            },
            "norm": norm,
            "cosine_to_gif": cosine,
            "max_abs_to_gif": max_abs,
            "command": command,
        }
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        print(name, json.dumps(report["cases"][name]), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--gif", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--backend", choices=("cuda", "metal", "native"), default="cuda"
    )
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if not 1 <= args.warmups <= 100 or not 1 <= args.iterations <= 10000:
        parser.error("require 1–100 warmups and 1–10000 measured iterations")
    run(args)


if __name__ == "__main__":
    main()
